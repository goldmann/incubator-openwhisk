/*
 * Copyright 2015-2016 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.containerpool.kubernetes

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

import spray.json._
import spray.json.DefaultJsonProtocol._
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionId
import whisk.core.container
import whisk.core.container.HttpUtils
import whisk.core.container.Interval
import whisk.core.container.RunResult
import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerFactory
import whisk.core.containerpool.InitializationError
import whisk.core.containerpool.WhiskContainerStartupError
import whisk.core.containerpool.docker.ContainerId
import whisk.core.containerpool.docker.ContainerIp
import whisk.core.entity.ActivationResponse
import whisk.core.entity.ByteSize
import whisk.core.entity.ExecManifest.ImageName
import whisk.core.entity.size._
import whisk.core.invoker.ActionLogDriver
import whisk.core.WhiskConfig
import whisk.http.Messages

object KubernetesContainer {
    /**
      * Creates a container running in kubernetes
      *
      * @param transid transaction creating the container
      * @param image image to create the container from
      * @param userProvidedImage whether the image is provided by the user
      *     or is an OpenWhisk provided image
      * @param labels labels to set on the container
      * @param name optional name for the container
      * @return a Future which either completes with a KubernetesContainer or one of two specific failures
      */
    def create(transid: TransactionId,
               image: String,
               userProvidedImage: Boolean = false,
               labels: Map[String, String] = Map(),
               name: Option[String] = None)(
                  implicit kubernetes: KubernetesApi, ec: ExecutionContext, log: Logging): Future[KubernetesContainer] = {
        implicit val tid = transid

        val podName = name.getOrElse("").replace("_", "-").replaceAll("[()]", "").toLowerCase()
        for {
            id <- kubernetes.run(image, podName, labels).recoverWith {
                case _ => Future.failed(WhiskContainerStartupError(s"Failed to run container with image '${image}'."))
            }
            ip <- kubernetes.inspectIPAddress(id).recoverWith {
                // remove the container immediately if inspect failed as
                // we cannot recover that case automatically
                case _ =>
                    kubernetes.rm(id)
                    Future.failed(WhiskContainerStartupError(s"Failed to obtain IP address of container '${id.asString}'."))
            }
        } yield new KubernetesContainer(id, ip)
    }
}

/**
  * Represents a container as run by kubernetes.
  *
  * This class contains OpenWhisk specific behavior and as such does not necessarily
  * use kubernetes commands to achieve the effects needed.
  *
  * @constructor
  * @param id the id of the container
  * @param ip the ip of the container
  */
class KubernetesContainer(id: ContainerId, ip: ContainerIp) (
    implicit kubernetes: KubernetesApi, ec: ExecutionContext, logger: Logging) extends Container with ActionLogDriver {

    /** The last read timestamp in the log file */
    private var lastTimestamp = ""

    protected val logsRetryCount = 15
    protected val logsRetryWait = 100.millis

    /** HTTP connection to the container, will be lazily established by callContainer */
    private var httpConnection: Option[HttpUtils] = None

    // no-op under Kubernetes
    def suspend()(implicit transid: TransactionId): Future[Unit] = Future.successful({})

    // no-op under Kubernetes
    def resume()(implicit transid: TransactionId): Future[Unit] = Future.successful({})

    def destroy()(implicit transid: TransactionId): Future[Unit] = kubernetes.rm(id)

    def initialize(initializer: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[container.Interval] = {
        val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_INIT, s"sending initialization to $id $ip")

        val body = JsObject("value" -> initializer)
        callContainer("/init", body, timeout, retry = true).andThen { // never fails
            case Success(r: RunResult) =>
                transid.finished(this, start.copy(start = r.interval.start), s"initialization result: ${r.toBriefString}", endTime = r.interval.end)
            case Failure(t) =>
                transid.failed(this, start, s"initializiation failed with $t")
        }.flatMap { result =>
            if (result.ok) {
                Future.successful(result.interval)
            } else if (result.interval.duration >= timeout) {
                Future.failed(InitializationError(result.interval, ActivationResponse.applicationError(Messages.timedoutActivation(timeout, true))))
            } else {
                Future.failed(InitializationError(result.interval, ActivationResponse.processInitResponseContent(result.response, logger)))
            }
        }
    }

    def run(parameters: JsObject, environment: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[(container.Interval, ActivationResponse)] = {
        val actionName = environment.fields.get("action_name").map(_.convertTo[String]).getOrElse("")
        val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_RUN, s"sending arguments to $actionName at $id $ip")

        val parameterWrapper = JsObject("value" -> parameters)
        val body = JsObject(parameterWrapper.fields ++ environment.fields)
        callContainer("/run", body, timeout, retry = false).andThen { // never fails
            case Success(r: RunResult) =>
                transid.finished(this, start.copy(start = r.interval.start), s"running result: ${r.toBriefString}", endTime = r.interval.end)
            case Failure(t) =>
                transid.failed(this, start, s"run failed with $t")
        }.map { result =>
            val response = if (result.interval.duration >= timeout) {
                ActivationResponse.applicationError(Messages.timedoutActivation(timeout, false))
            } else {
                ActivationResponse.processRunResponseContent(result.response, logger)
            }

            (result.interval, response)
        }
    }

    def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Future[Vector[String]] = {

        def readLogs(retries: Int): Future[Vector[String]] = {
            kubernetes.logs(id, lastTimestamp).flatMap { rawLog =>
                val lastTS = rawLog.lines.toSeq.lastOption.getOrElse("""{"time":""}""").parseJson.asJsObject.fields("time").convertTo[String]
                val (isComplete, isTruncated, formattedLogs) = processJsonDriverLogContents(rawLog, waitForSentinel, limit)

                if (retries > 0 && !isComplete && !isTruncated) {
                    logger.info(this, s"log cursor advanced but missing sentinel, trying $retries more times")
                    Thread.sleep(logsRetryWait.toMillis)
                    readLogs(retries - 1)
                } else {
                    lastTimestamp = lastTS
                    Future.successful(formattedLogs)
                }
            }.andThen {
                case Failure(e) =>
                    logger.error(this, s"Failed to obtain logs of ${id.asString}: ${e.getClass} - ${e.getMessage}")
            }
        }

        readLogs(logsRetryCount)
    }

    /**
      * Makes an HTTP request to the container.
      *
      * Note that `http.post` will not throw an exception, hence the generated Future cannot fail.
      *
      * @param path relative path to use in the http request
      * @param body body to send
      * @param timeout timeout of the request
      * @param retry whether or not to retry the request
      */
    protected def callContainer(path: String, body: JsObject, timeout: FiniteDuration, retry: Boolean = false): Future[RunResult] = {
        val started = Instant.now()
        val http = httpConnection.getOrElse {
            val conn = new HttpUtils(s"${ip.asString}:8080", timeout, 1.MB)
            httpConnection = Some(conn)
            conn
        }
        Future {
            http.post(path, body, retry)
        }.map { response =>
            val finished = Instant.now()
            RunResult(Interval(started, finished), response)
        }
    }
}

class KubernetesContainerFactory(label: String, config: WhiskConfig)(implicit ec: ExecutionContext, logger: Logging) extends ContainerFactory {

    implicit val kubernetes = new KubernetesClient()(ec)

    def cleanup() = {
        val cleaning = kubernetes.rm("invoker", label)(TransactionId.invokerNanny)
        Await.ready(cleaning, 30.seconds)
    }

    def create(tid: TransactionId, name: String, actionImage: ImageName, userProvidedImage: Boolean, memory: ByteSize): Future[Container] = {
        val image = if (userProvidedImage) {
            actionImage.publicImageName
        } else {
            actionImage.localImageName(config.dockerRegistry, config.dockerImagePrefix, Some(config.dockerImageTag))
        }

        KubernetesContainer.create(
            tid,
            image = image,
            userProvidedImage = userProvidedImage,
            labels = Map("invoker" -> label),
            name = Some(name))
    }
}
