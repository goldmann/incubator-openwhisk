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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatterBuilder

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

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._
import spray.json.DefaultJsonProtocol._
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import okhttp3.{Call, Callback, Request, Response}
import okio.BufferedSource

import scala.annotation.tailrec
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

  def logs(id: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean = false)(
    implicit transid: TransactionId): Source[ByteString, Any] = {

    log.debug(this, "Parsing logs from Kubernetes Graph Stage…")

    Source
      .fromGraph(new KubernetesRestLogSourceStage(id, sinceTime, waitForSentinel))
      .log("foobar")

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

object KubernetesClient {

  //%Y-%m-%dT%H:%M:%S.%N%z
  val K8SDateTimeFormat = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .appendPattern("u-MM-dd")
    .appendLiteral('T')
    .appendPattern("HH:mm:ss[.n]")
    .appendLiteral('Z')
    .toFormatter()

}

trait KubernetesApi {
  def run(name: String, image: String, args: Seq[String] = Seq.empty[String])(
    implicit transid: TransactionId): Future[ContainerId]

  def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress]

  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit]

  def logs(containerId: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean = false)(
    implicit transid: TransactionId): Source[ByteString, Any]
}

object KubernetesRestLogSourceStage {

  import KubernetesClient.K8SDateTimeFormat

  def constructPath(namespace: String, containerId: String): Path =
    Path / "api" / "v1" / "namespaces" / namespace / "pods" / containerId / "log"

  def constructQuery(sinceTime: Option[LocalDateTime], waitForSentinel: Boolean): Query = {

    val qB = Map.newBuilder[String, String]
    qB += "timestamps" → "true"
    qB ++= sinceTime.map(time => "sinceTime" -> time.format(KubernetesClient.K8SDateTimeFormat))

    Query(qB.result())
  }

  @tailrec
  def readLines(src: BufferedSource,
                lastTimestamp: Option[LocalDateTime],
                lines: Seq[JsObject] = Seq.empty[JsObject]): (Option[LocalDateTime], Seq[JsObject]) = {

    if (!src.exhausted()) {
      (for {
        l <- Option(src.readUtf8Line()) if !l.isEmpty
        p = l.indexOf(" ")
        // Kubernetes is ignoring nanoseconds in sinceTime, so we have to filter additionally here
        ts = l.substring(0, p)
        tsDate <- Try(LocalDateTime.parse(ts, K8SDateTimeFormat)).toOption if isRelevantLogLine(lastTimestamp, tsDate)
        msg = l.substring(p + 1)
        stream = "stdout" // TODO - when we can distinguish stderr: https://github.com/kubernetes/kubernetes/issues/28167
      } yield {
        tsDate -> JsObject("log" -> msg.toJson, "stream" -> stream.toJson, "time" -> ts.toJson)
      }) match {
        case Some((latestTS, js)) =>
          readLines(src, Some(latestTS), lines :+ js)
        case None =>
          // we may have skipped a line for filtering conditions only; keep going
          readLines(src, lastTimestamp, lines)
      }
    } else {
      src.close()
      (lastTimestamp, lines)
    }

  }

  def isRelevantLogLine(lastTimestamp: Option[LocalDateTime], newTimestamp: LocalDateTime): Boolean =
    lastTimestamp match {
      case Some(last) =>
        newTimestamp.isAfter(last)
      case None =>
        true
    }

}

final class KubernetesRestLogSourceStage(id: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
  implicit val kubeRestClient: DefaultKubernetesClient)
    extends GraphStage[SourceShape[ByteString]] { stage =>

  import KubernetesRestLogSourceStage._

  val out = Outlet[ByteString]("K8SHttpLogging.out")

  override val shape: SourceShape[ByteString] = SourceShape.of(out)

  override protected def initialAttributes: Attributes = Attributes.name("KubernetesHttpLogSource")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogicWithLogging(shape) { logic =>

      private val queue = mutable.Queue.empty[ByteString]
      private var lastTimestamp = sinceTime

      def fetchLogs(): Unit =
        try {
          val path = constructPath(kubeRestClient.getNamespace, id.asString)
          val query = constructQuery(lastTimestamp, waitForSentinel)

          log.debug("* Fetching K8S HTTP Logs w/ Path: {} Query: {}", path, query)

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
          log.warning("* Logging socket to Kubernetes timed out.") // this should only happen with follow behavior
        case _ =>
          log.error(e, "* Retrieving the logs from Kubernetes failed.")
      }

      val emitCallback: AsyncCallback[ByteString] = getAsyncCallback[ByteString] { line =>
        if (isAvailable(out)) {
          pushLines(line)
        } else {
          queue.enqueue(line)
        }
      }

      class LogFetchCallback extends Callback {

        override def onFailure(call: Call, e: IOException): Unit = logic.onFailure(e)

        override def onResponse(call: Call, response: Response): Unit =
          try {
            val (latestTS, lines) = readLines(response.body.source(), lastTimestamp)
            lastTimestamp = latestTS
            emitCallback.invoke(ByteString(lines.mkString("", "\n", "\n"))) //  trailing newline is necessary or the frame won't be decoded and break the akka stream

          } catch {
            case NonFatal(e) =>
              log.error(e, "* Reading Kubernetes HTTP Response failed.")
              logic.onFailure(e)
              throw e
          }
      }

      def pushLines(lines: ByteString): Unit = {
        log.debug("* Pushing a chunk of kubernetes logging: {}", lines.utf8String)
        push(out, lines)
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            // if we still have lines queued up, return those; else make a new HTTP read.
            if (queue.nonEmpty)
              pushLines(queue.dequeue())
            else
              fetchLogs()
          }
        })
    }
}
