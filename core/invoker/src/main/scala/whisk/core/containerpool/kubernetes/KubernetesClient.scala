/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.containerpool.kubernetes

import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.event.Logging.{ErrorLevel, InfoLevel}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.Uri.Query
import akka.stream.scaladsl.Source
import akka.util.ByteString
import pureconfig.loadConfigOrThrow
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionId
import whisk.core.ConfigKeys
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.ContainerAddress
import whisk.core.containerpool.docker.ProcessRunner
import whisk.core.containerpool.logging.LogLine
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._

import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import okhttp3.Request

/**
 * Configuration for kubernetes client command timeouts.
 */
case class KubernetesClientTimeoutConfig(run: Duration, rm: Duration, inspect: Duration, logs: Duration)

/**
 * Serves as interface to the kubectl CLI tool.
 *
 * Be cautious with the ExecutionContext passed to this, as the
 * calls to the CLI are blocking.
 *
 * You only need one instance (and you shouldn't get more).
 */
class KubernetesClient(
  timeouts: KubernetesClientTimeoutConfig = loadConfigOrThrow[KubernetesClientTimeoutConfig](
    ConfigKeys.kubernetesTimeouts))(executionContext: ExecutionContext)(implicit log: Logging, as: ActorSystem)
    extends KubernetesApi
    with ProcessRunner {
  implicit private val ec = executionContext
  implicit private val kubeRestClient = new DefaultKubernetesClient(
    new ConfigBuilder()
      .withConnectionTimeout(timeouts.logs.toMillis.toInt)
      .withRequestTimeout(timeouts.logs.toMillis.toInt)
      .build())

  // Determines how to run kubectl. Failure to find a kubectl binary implies
  // a failure to initialize this instance of KubernetesClient.
  protected def findKubectlCmd(): String = {
    val alternatives = List("/usr/bin/kubectl", "/usr/local/bin/kubectl")
    val kubectlBin = Try {
      alternatives.find(a => Files.isExecutable(Paths.get(a))).get
    } getOrElse {
      throw new FileNotFoundException(s"Couldn't locate kubectl binary (tried: ${alternatives.mkString(", ")}).")
    }
    kubectlBin
  }
  protected val kubectlCmd = Seq(findKubectlCmd)

  def run(name: String, image: String, args: Seq[String] = Seq.empty[String])(
    implicit transid: TransactionId): Future[ContainerId] = {
    runCmd(Seq("run", name, s"--image=$image") ++ args, timeouts.run)
      .map(_ => ContainerId(name))
  }

  def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress] = {
    Future {
      blocking {
        val pod =
          kubeRestClient.pods().withName(id.asString).waitUntilReady(timeouts.inspect.length, timeouts.inspect.unit)
        ContainerAddress(pod.getStatus().getPodIP())
      }
    }.recoverWith {
      case e =>
        log.error(this, s"Failed to get IP of Pod '${id.asString}' within timeout: ${e.getClass} - ${e.getMessage}")
        Future.failed(new Exception(s"Failed to get IP of Pod '${id.asString}'"))
    }
  }

  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("delete", "--now", "pod", id.asString), timeouts.rm).map(_ => ())

  def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("delete", "--now", "pod", "-l", s"$key=$value"), timeouts.rm).map(_ => ())

  def logs(id: ContainerId, sinceTime: Option[String])(implicit transid: TransactionId): Source[ByteString, Any] = {

    Source.fromFuture(Future {
      fetchHTTPLogs(id, sinceTime)
    }.flatMap(identity).map { output =>
      // The k8s logs api currently has less granularity than the
      // timestamp we're passing as the 'sinceTime' argument, so we
      // may need to filter logs from previous activations on a warm
      // runtime pod
      val original = output.lines.toSeq
      val result = sinceTime match {
        // cold pod, just take the log output
        case None => original
        // there was an activation already on this pod
        case Some(time) =>
          val filtered = original.dropWhile(!_.startsWith(time))
          // filtered can be empty iff the previous timestamp is not
          // in the collection, which could happen if the granularity
          // of the API ever matches that of the timestamp. Take the
          // original output in that case. If nonEmpty, its head will
          // contain the last line of the previous activation.
          if (filtered.nonEmpty) filtered.drop(1) else original
      }
      // map the logs to the docker json file format expected by
      // ActionLogDriver.processJsonDriverLogContents
      ByteString(result.map { line =>
        val pos = line.indexOf(" ")
        val ts = line.substring(0, pos)
        val msg = line.substring(pos + 1)
        // TODO - when we can distinguish stderr: https://github.com/kubernetes/kubernetes/issues/28167
        val stream = "stdout"
        LogLine(ts, stream, msg).toJson.compactPrint + "\n"
      }.mkString)
    })
  }

  protected def fetchHTTPLogs(id: ContainerId, sinceTime: Option[String]) = {
    blocking {
      val path = Path / "api" / "v1" / "namespaces" / kubeRestClient.getNamespace / "pods" / id.asString / "log"
      val query = Seq("timestamps" -> true.toString) ++ sinceTime.map(time => "sinceTime" -> time)
      val url = Uri(kubeRestClient.getMasterUrl.toString)
        .withPath(path)
        .withQuery(Query(query: _*))

      val request = new Request.Builder().get().url(url.toString).build
      val response = kubeRestClient.getHttpClient.newCall(request).execute
      if (response.isSuccessful) {
        Future.successful(response.body.string)
      } else {
        Future.failed(
          new Exception(s"Kubernetes API returned HTTP status ${response.code} when trying to retrieve pod logs"))
      }
    }
  }

  private def runCmd(args: Seq[String], timeout: Duration)(implicit transid: TransactionId): Future[String] = {
    val cmd = kubectlCmd ++ args
    val start = transid.started(
      this,
      LoggingMarkers.INVOKER_KUBECTL_CMD(args.head),
      s"running ${cmd.mkString(" ")} (timeout: $timeout)",
      logLevel = InfoLevel)
    executeProcess(cmd, timeout).andThen {
      case Success(_) => transid.finished(this, start)
      case Failure(t) => transid.failed(this, start, t.getMessage, ErrorLevel)
    }
  }
}

trait KubernetesApi {
  def run(name: String, image: String, args: Seq[String] = Seq.empty[String])(
    implicit transid: TransactionId): Future[ContainerId]

  def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress]

  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit]

  def logs(containerId: ContainerId, sinceTime: Option[String])(
    implicit transid: TransactionId): Source[ByteString, Any]
}
