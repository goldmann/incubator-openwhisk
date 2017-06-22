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

package whisk.core.containerpool.kubernetes.test

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import common.StreamLogging
import whisk.common.LogMarker
import whisk.common.LoggingMarkers.INVOKER_KUBECTL_CMD
import whisk.common.TransactionId
import whisk.core.containerpool.docker.ContainerId
import whisk.core.containerpool.docker.ContainerIp
import whisk.core.containerpool.kubernetes.KubernetesClient

@RunWith(classOf[JUnitRunner])
class KubernetesClientTests extends FlatSpec with Matchers with StreamLogging with BeforeAndAfterEach {

    override def beforeEach = stream.reset()

    implicit val transid = TransactionId.testing
    val id = ContainerId("Id")

    def await[A](f: Future[A], timeout: FiniteDuration = 500.milliseconds) = Await.result(f, timeout)

    val kubectlCommand = "kubectl"

    /** Returns a KubernetesClient with a mocked result for 'executeProcess' */
    def kubernetesClient(execResult: Future[String]) = new KubernetesClient()(global) {
        override val kubectlCmd = Seq(kubectlCommand)
        override def executeProcess(args: String*)(implicit ec: ExecutionContext) = execResult
    }

    behavior of "KubernetesClient"

    it should "throw NoSuchElementException if specified network does not exist when using 'inspectIPAddress'" in {
        val client = kubernetesClient { Future.successful("<no value>") }

        a[NoSuchElementException] should be thrownBy await(client.inspectIPAddress(id))
    }

    it should "write proper log markers on a successful command" in {
        // a dummy string works here as we do not assert any output
        // from the methods below
        val stdout = "stdout"
        val client = kubernetesClient { Future.successful(stdout) }

        /** Awaits the command and checks for proper logging. */
        def runAndVerify(f: Future[_], cmd: String, args: Seq[String] = Seq.empty[String]) = {
            val result = await(f)

            logLines.head should include((Seq(kubectlCommand, cmd) ++ args).mkString(" "))

            val start = LogMarker.parse(logLines.head)
            start.token shouldBe INVOKER_KUBECTL_CMD(cmd)

            val end = LogMarker.parse(logLines.last)
            end.token shouldBe INVOKER_KUBECTL_CMD(cmd).asFinish

            stream.reset()
            result
        }

        runAndVerify(client.rm(id), "rm", Seq("-f", id.asString))

        val network = "userland"
        val inspectArgs = Seq("--format", s"{{.NetworkSettings.Networks.${network}.IPAddress}}", id.asString)
        runAndVerify(client.inspectIPAddress(id), "inspect", inspectArgs) shouldBe ContainerIp(stdout)

        val image = "image"
        val runArgs = Seq("--memory", "256m", "--cpushares", "1024")
        runAndVerify(client.run(image, "name"), "run", Seq("-d") ++ runArgs ++ Seq(image)) shouldBe ContainerId(stdout)
    }

    it should "write proper log markers on a failing command" in {
        val client = kubernetesClient { Future.failed(new RuntimeException()) }

        /** Awaits the command, asserts the exception and checks for proper logging. */
        def runAndVerify(f: Future[_], cmd: String) = {
            a[RuntimeException] should be thrownBy await(f)

            val start = LogMarker.parse(logLines.head)
            start.token shouldBe INVOKER_KUBECTL_CMD(cmd)

            val end = LogMarker.parse(logLines.last)
            end.token shouldBe INVOKER_KUBECTL_CMD(cmd).asError

            stream.reset()
        }

        runAndVerify(client.rm(id), "rm")
        runAndVerify(client.inspectIPAddress(id), "inspect")
        runAndVerify(client.run("image", "name"), "run")
    }
}
