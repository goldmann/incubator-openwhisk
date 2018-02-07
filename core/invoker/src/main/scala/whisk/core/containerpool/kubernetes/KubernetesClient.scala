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

import java.io.{FileNotFoundException, IOException}
import java.net.SocketTimeoutException
import java.nio.file.Files
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.event.Logging.{ErrorLevel, InfoLevel}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.Uri.Query
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage._
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
import okhttp3.{Call, Callback, Request, Response}

import scala.collection.mutable
import scala.util.control.NonFatal

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

    runCmd(runArgs, timeouts.run)
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

  def logs(id: ContainerId, sinceTime: Option[String], waitForSentinel: Boolean = false)(
    implicit transid: TransactionId): Source[ByteString, Any] = {

    log.debug(this, "Parsing logs from Kubernetes Graph Stage…")

    Source
      .fromGraph(new KubernetesRestLogSourceStage(id, sinceTime, waitForSentinel))

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
  def run(image: String, name: String, environment: Map[String, String] = Map(), labels: Map[String, String] = Map())(
    implicit transid: TransactionId): Future[ContainerId]

  def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress]

  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit]

  def logs(containerId: ContainerId, sinceTime: Option[String], waitForSentinel: Boolean = false)(
    implicit transid: TransactionId): Source[ByteString, Any]
}

final class KubernetesRestLogSourceStage(id: ContainerId, sinceTime: Option[String], waitForSentinel: Boolean)(
  implicit val kubeRestClient: DefaultKubernetesClient)
    extends GraphStage[SourceShape[ByteString]] {
  stage =>
  val out = Outlet[ByteString]("K8SHttpLogging.out")

  override val shape: SourceShape[ByteString] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = Attributes.name("KubernetesHttpLogSource")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogicWithLogging(shape) {
      logic =>

      private val queue = mutable.Queue.empty[LogLine]

      final override def preStart(): Unit =
        try {
          val path = Path / "api" / "v1" / "namespaces" / kubeRestClient.getNamespace / "pods" / id.asString / "log"
          val qB = Map.newBuilder[String, String]
          qB += "timestamps" → "true"
          qB ++= sinceTime.map("sinceTime" → _)
          if (waitForSentinel) qB += "follow" → "true"

          val query = Query(qB.result())

          log.debug("Fetching K8S HTTP Logs w/ Query: {}", query)

          val url = Uri(kubeRestClient.getMasterUrl.toString)
            .withPath(path)
            .withQuery(query)

          val request = new Request.Builder().get().url(url.toString).build

          kubeRestClient.getHttpClient.newCall(request).enqueue(new LogFetchCallback())
        } catch {
          case NonFatal(e) =>
            onFailure(e)
            throw e
        }

      def onFailure(e: Throwable): Unit = e match {
        case _: SocketTimeoutException =>
          log.warning("Logging socket to Kubernetes timed out. Likely not an error.")
        case _ =>
          log.error(e, "Retrieving the logs from Kubernetes failed.")
      }

      val emitCallback: AsyncCallback[LogLine] = getAsyncCallback[LogLine] { line =>
        if (isAvailable(out)) {
          pushLine(line)
        } else {
          queue.enqueue(line)
        }
      }

      class LogFetchCallback extends Callback {

        override def onFailure(call: Call, e: IOException): Unit = logic.onFailure(e)

        override def onResponse(call: Call, response: Response): Unit =
          try {
            val src = response.body.source

            // we need to drop any "old" log lines, i.e. any before the sinceTime variable
            // it might be better longterm to instantiate the since Time as a date, timestamp, or milliseconds
            // for quick less than / greater than compare
            var foundRelevant = false

            // todo - right now this exhausts the initial return and exits; for sentinel "mode" we need to keep going… (needs figuring out)
            while (!src.exhausted) { // todo - FIXME this is not the best idiomatic way to do this but for the life of me can't remember the better approach
              val line: Option[LogLine] = for {
                l <- Option(src.readUtf8Line()) if !l.isEmpty
                _ = log.debug("* Raw line of k8s log: {} (sinceTime: {})", l, sinceTime)
                st <- sinceTime.orElse(Some("")) if l.startsWith(st) || foundRelevant // todo - fix me to be less janky
                _ = log.debug("* sinceTime: {}", st)
                p = l.indexOf(" ")
                _ = log.debug("* first space pos: {}", p)
                ts = l.substring(0, p)
                _ = log.debug("* log line timestamp: {}", ts)
                msg = l.substring(p + 1)
                _ = log.debug("* log message: {}", msg)
              } yield {
                foundRelevant = true
                LogLine(ts, "stdout", msg) // TODO - when we can distinguish stderr: https://github.com/kubernetes/kubernetes/issues/28167
              }

              log.debug("Received and parsed a LogLine: {}", line)

              for (l <- line) emitCallback.invoke(l)
            }

            src.close()

          } catch {
            case NonFatal(e) =>
              logic.onFailure(e)
              throw e
          }
      }

      def pushLine(line: LogLine): Unit = {
        val json = line.toJson.compactPrint
        log.debug("Pushing a line of JSON for kubernetes logging: {}", json)
        push(out, ByteString(json))
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = if (queue.nonEmpty) pushLine(queue.dequeue())

          override def onDownstreamFinish(): Unit = {
            setKeepGoing(waitForSentinel) // todo - this is where we should probably be dealing w/ the "follow" mode of sentinel; double check
            super.onDownstreamFinish()
          }
        })
    }
}
