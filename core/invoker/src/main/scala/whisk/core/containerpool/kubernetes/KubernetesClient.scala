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

import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Paths

import akka.event.Logging.ErrorLevel
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionId
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.ContainerAddress
import whisk.core.containerpool.docker.ProcessRunner

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Serves as interface to the kubectl CLI tool.
  *
  * Be cautious with the ExecutionContext passed to this, as the
  * calls to the CLI are blocking.
  *
  * You only need one instance (and you shouldn't get more).
  */
class KubernetesClient()(executionContext: ExecutionContext)(implicit log: Logging)
        extends KubernetesApi with ProcessRunner {
    implicit private val ec = executionContext

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

    def run(image: String, name: String, labels: Map[String, String] = Map())(implicit transid: TransactionId): Future[ContainerId] = {
        val run = runCmd("run", name, "--image", image, "--restart", "Never").map {_ => name}.map(ContainerId.apply)
        if (labels.isEmpty) {
            run
        } else {
            run.flatMap { id => 
                val args = Seq("label", "pod", id.asString) ++
                    labels.map {
                        case (key, value) => s"$key=$value"
                    }
                runCmd(args: _*).map {_ => id}
            }
        }
    }

    def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerIp] = getIP(id)

    def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
        runCmd("delete", "--now", "pod", id.asString).map(_ => ())

    def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit] =
        runCmd("delete", "--now", "pod", "-l", s"$key=$value").map(_ => ())

    private def getIP(id: ContainerId, tries: Int = 25)(implicit transid: TransactionId): Future[ContainerIp] = {
        runCmd("get", "pod", id.asString, "--template", "{{.status.podIP}}").flatMap {
            _ match {
                case "<no value>" =>
                    if (tries > 0) {
                        Thread.sleep(100)
                        getIP(id, tries - 1)
                    } else {
                        Future.failed(new NoSuchElementException)
                    }
                case stdout => Future.successful(ContainerIp(stdout))
            }
        }
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
    def run(image: String, name: String, labels: Map[String, String] = Map())(implicit transid: TransactionId): Future[ContainerId]

    def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerIp]

    def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

    def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit]
}
