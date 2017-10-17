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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure

import spray.json._
import spray.json.DefaultJsonProtocol._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.containerpool.Container
import whisk.core.containerpool.WhiskContainerStartupError
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.ContainerAddress
import whisk.core.containerpool.docker.DockerActionLogDriver
import whisk.core.entity.ByteSize

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
             environment: Map[String, String] = Map(),
             labels: Map[String, String] = Map(),
             name: Option[String] = None)(implicit kubernetes: KubernetesApi,
                                          ec: ExecutionContext,
                                          log: Logging): Future[KubernetesContainer] = {
    implicit val tid = transid

    val podName = name.getOrElse("").replace("_", "-").replaceAll("[()]", "").toLowerCase()
    for {
      id <- kubernetes.run(image, podName, environment, labels).recoverWith {
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
class KubernetesContainer(protected val id: ContainerId, protected val addr: ContainerAddress)(
  implicit kubernetes: KubernetesApi,
  protected val ec: ExecutionContext,
  protected val logging: Logging)
    extends Container
    with DockerActionLogDriver {

  /** The last read timestamp in the log file */
  private var lastTimestamp = ""

  protected val logsRetryCount = 15
  protected val logsRetryWait = 100.millis

  // no-op under Kubernetes
  def suspend()(implicit transid: TransactionId): Future[Unit] = Future.successful({})

  // no-op under Kubernetes
  def resume()(implicit transid: TransactionId): Future[Unit] = Future.successful({})

  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    super.destroy()
    kubernetes.rm(id)
  }

  def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Future[Vector[String]] = {

    def readLogs(retries: Int): Future[Vector[String]] = {
      kubernetes
        .logs(id, lastTimestamp)
        .flatMap { rawLog =>
          val lastTS = rawLog.lines.toSeq.lastOption
            .getOrElse("""{"time":""}""")
            .parseJson
            .asJsObject
            .fields("time")
            .convertTo[String]
          val (isComplete, isTruncated, formattedLogs) = processJsonDriverLogContents(rawLog, waitForSentinel, limit)

          if (retries > 0 && !isComplete && !isTruncated) {
            logging.info(this, s"log cursor advanced but missing sentinel, trying $retries more times")
            Thread.sleep(logsRetryWait.toMillis)
            readLogs(retries - 1)
          } else {
            lastTimestamp = lastTS
            Future.successful(formattedLogs)
          }
        }
        .andThen {
          case Failure(e) =>
            logging.error(this, s"Failed to obtain logs of ${id.asString}: ${e.getClass} - ${e.getMessage}")
        }
    }

    readLogs(logsRetryCount)
  }
}
