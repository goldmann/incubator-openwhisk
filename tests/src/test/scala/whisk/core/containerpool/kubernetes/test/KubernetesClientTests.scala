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

package whisk.core.containerpool.kubernetes.test

import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers
import org.scalatest.time.{Seconds, Span}
import common.{StreamLogging, WskActorSystem}
import whisk.common.LogMarker
import whisk.common.LoggingMarkers.INVOKER_KUBECTL_CMD
import whisk.common.TransactionId
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.kubernetes.KubernetesClient
import whisk.core.containerpool.docker.ProcessRunningException

@RunWith(classOf[JUnitRunner])
class KubernetesClientTests
    extends FlatSpec
    with Matchers
    with StreamLogging
    with BeforeAndAfterEach
    with Eventually
    with WskActorSystem {

  override def beforeEach = stream.reset()

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(5, Seconds)))

  implicit val transid = TransactionId.testing
  val id = ContainerId("55db56ee082239428b27d3728b4dd324c09068458aad9825727d5bfc1bba6d52")

  val commandTimeout = 500.milliseconds
  def await[A](f: Future[A], timeout: FiniteDuration = commandTimeout) = Await.result(f, timeout)

  val kubectlCommand = "kubectl"

  /** Returns a KubernetesClient with a mocked result for 'executeProcess' */
  def kubernetesClient(execResult: => Future[String]) = new KubernetesClient()(global) {
    override def findKubectlCmd() = kubectlCommand
    override def executeProcess(args: Seq[String], timeout: Duration)(implicit ec: ExecutionContext, as: ActorSystem) =
      execResult
  }

  behavior of "KubernetesClient"

  it should "write proper log markers on a successful command" in {
    // a dummy string works here as we do not assert any output
    // from the methods below
    val stdout = "stdout"
    val dc = kubernetesClient { Future.successful(stdout) }

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

    runAndVerify(dc.rm(id), "delete", Seq("--now", "pod", id.asString))

    val image = "image"
    val name = "name"
    val expected =
      Seq(name, "--image", image, "--generator", "run-pod/v1", "--restart", "Always", "--limits", "memory=256Mi")
    runAndVerify(dc.run(image, name), "run", expected) shouldBe ContainerId(name)
  }

  it should "write proper log markers on a failing command" in {
    val dc = kubernetesClient { Future.failed(new RuntimeException()) }

    /** Awaits the command, asserts the exception and checks for proper logging. */
    def runAndVerify(f: Future[_], cmd: String) = {
      a[RuntimeException] should be thrownBy await(f)

      val start = LogMarker.parse(logLines.head)
      start.token shouldBe INVOKER_KUBECTL_CMD(cmd)

      val end = LogMarker.parse(logLines.last)
      end.token shouldBe INVOKER_KUBECTL_CMD(cmd).asError

      stream.reset()
    }

    runAndVerify(dc.rm(id), "delete")
    runAndVerify(dc.run("image", "name"), "run")
  }

  it should "fail with ProcessRunningException when run returns with exit code !=125 or no container ID" in {
    def runAndVerify(pre: ProcessRunningException, clue: String) = {
      val dc = kubernetesClient { Future.failed(pre) }
      withClue(s"${clue} - exitCode = ${pre.exitCode}, stdout = '${pre.stdout}', stderr = '${pre.stderr}': ") {
        the[ProcessRunningException] thrownBy await(dc.run("image", "name")) shouldBe pre
      }
    }

    Seq[(ProcessRunningException, String)](
      (ProcessRunningException(126, id.asString, "Unknown command"), "Exit code not 125"),
      (ProcessRunningException(125, "", "Unknown flag: --foo"), "No container ID"),
      (ProcessRunningException(1, "", ""), "Exit code not 125 and no container ID")).foreach {
      case (pre, clue) => runAndVerify(pre, clue)
    }
  }
}
