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
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths

import akka.event.Logging.ErrorLevel
import akka.stream.scaladsl.Source
import akka.util.ByteString
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionId
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.ContainerAddress
import whisk.core.containerpool.docker.ProcessRunner

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._
import spray.json.DefaultJsonProtocol._
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import io.fabric8.kubernetes.client.utils.URLUtils
import okhttp3.Request

/**
 * Serves as interface to the kubectl CLI tool.
 *
 * Be cautious with the ExecutionContext passed to this, as the
 * calls to the CLI are blocking.
 *
 * You only need one instance (and you shouldn't get more).
 */
class KubernetesClient()(executionContext: ExecutionContext)(implicit log: Logging)
    extends KubernetesApi
    with ProcessRunner {
  implicit private val ec = executionContext
  implicit private val kubeRestClient = new DefaultKubernetesClient

  // Determines how to run kubectl. Failure to find a kubectl binary implies
  // a failure to initialize this instance of KubernetesClient.
  protected val kubectlCmd: Seq[String] = {
    val alternatives = List("/usr/bin/kubectl", "/usr/local/bin/kubectl")

    val kubectlBin = Try {
      alternatives.find(a => Files.isExecutable(Paths.get(a))).get
    } getOrElse {
      throw new FileNotFoundException(s"Couldn't locate kubectl binary (tried: ${alternatives.mkString(", ")}).")
    }

    Seq(kubectlBin)
  }

  def run(image: String, name: String, environment: Map[String, String] = Map(), labels: Map[String, String] = Map())(
    implicit transid: TransactionId): Future[ContainerId] = {
    val environmentArgs = environment.flatMap {
      case (key, value) => Seq("--env", s"$key=$value")
    }.toSeq

    val labelArgs = labels.map {
      case (key, value) => s"$key=$value"
    } match {
      case Seq() => Seq()
      case pairs => Seq("-l") ++ pairs
    }

    val runArgs = Seq(
      "run",
      name,
      "--image",
      image,
      "--generator",
      "run-pod/v1",
      "--restart",
      "Always",
      "--limits",
      "memory=256Mi") ++ environmentArgs ++ labelArgs

    runCmd(runArgs: _*)
      .map { _ =>
        name
      }
      .map(ContainerId.apply)
  }

  def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress] = {
    Future {
      blocking {
        val pod = kubeRestClient.pods().withName(id.asString).waitUntilReady(30, SECONDS)
        ContainerAddress(pod.getStatus().getPodIP())
      }
    }.recoverWith {
      case e =>
        log.error(this, s"Failed to get IP of Pod '${id.asString}' within timeout: ${e.getClass} - ${e.getMessage}")
        Future.failed(new Exception(s"Failed to get IP of Pod '${id.asString}'"))
    }
  }

  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd("delete", "--now", "pod", id.asString).map(_ => ())

  def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit] =
    runCmd("delete", "--now", "pod", "-l", s"$key=$value").map(_ => ())

  def logs(id: ContainerId, sinceTime: String)(implicit transid: TransactionId): Source[ByteString, Any] = {

    Source.fromFuture(Future {
      blocking {
        val sinceTimePart = if (!sinceTime.isEmpty) {
          s"&sinceTime=${sinceTime}"
        } else ""
        val url = new URL(
          URLUtils.join(
            kubeRestClient.getMasterUrl.toString,
            "api",
            "v1",
            "namespaces",
            kubeRestClient.getNamespace,
            "pods",
            id.asString,
            "log?timestamps=true" ++ sinceTimePart))
        val request = new Request.Builder().get().url(url).build
        val response = kubeRestClient.getHttpClient.newCall(request).execute
        if (!response.isSuccessful) {
          Future.failed(
            new Exception(s"Kubernetes API returned HTTP status ${response.code} when trying to retrieve pod logs"))
        }
        response.body.string
      }
    }.map { output =>
      val original = output.lines.toSeq
      val relevant = original.dropWhile(s => !s.startsWith(sinceTime))
      val result =
        if (!relevant.isEmpty && original.size > relevant.size) {
          // drop matching timestamp from previous activation
          relevant.drop(1)
        } else {
          original
        }
      // map the logs to the docker json file format expected by
      // ActionLogDriver.processJsonDriverLogContents
      // TODO - jcrossley - you'll probably want to clean up / reimplement the logic intended by this with the new format
      //var sentinelSeen = false
      ByteString(
        result
          .map { line =>
            val pos = line.indexOf(" ")
            val ts = line.substring(0, pos)
            val msg = line.substring(pos + 1)
            // TODO - can we get the proper stream name from kubernetes? Some stuff is stderr
            val stream = "stdout"
            JsObject("log" -> msg.toJson, "stream" -> stream.toJson, "time" -> ts.toJson)
          }
          .mkString("\n") + "\n" // trailing newline is necessary or the frame won't be decoded and break the akka stream
      )
    })
  }

  private def runCmd(args: String*)(implicit transid: TransactionId): Future[String] = {
    val cmd = kubectlCmd ++ args
    val start = transid.started(this, LoggingMarkers.INVOKER_KUBECTL_CMD(args.head), s"running ${cmd.mkString(" ")}")
    executeProcess(cmd: _*).andThen {
      case Success(_) => transid.finished(this, start)
      case Failure(t) => transid.failed(this, start, t.getMessage, ErrorLevel)
    }
  }
}

trait KubernetesApi {
  def run(image: String, name: String, environment: Map[String, String] = Map(), labels: Map[String, String] = Map())(
    implicit transid: TransactionId): Future[ContainerId]

  def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress]

  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit]

  def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Source[ByteString, Any]
}
