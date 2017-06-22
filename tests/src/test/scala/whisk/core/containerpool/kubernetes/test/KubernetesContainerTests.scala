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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpec
import org.scalatest.Inspectors._
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import common.StreamLogging
import spray.json._
import whisk.common.LoggingMarkers._
import whisk.common.LogMarker
import whisk.common.TransactionId
import whisk.core.container.Interval
import whisk.core.container.RunResult
import whisk.core.containerpool._
import whisk.core.containerpool.docker._
import whisk.core.containerpool.kubernetes._
import whisk.core.entity.ActivationResponse
import whisk.core.entity.ActivationResponse.ContainerResponse
import whisk.core.entity.ActivationResponse.Timeout
import whisk.core.entity.size._
import whisk.core.invoker.ActionLogDriver
import whisk.core.invoker.LogLine
import whisk.http.Messages

/**
 * Unit tests for ContainerPool schedule
 */
@RunWith(classOf[JUnitRunner])
class KubernetesContainerTests extends FlatSpec
    with Matchers
    with MockFactory
    with StreamLogging
    with BeforeAndAfterEach {

    override def beforeEach() = {
        stream.reset()
    }

    /** Awaits the given future, throws the exception enclosed in Failure. */
    def await[A](f: Future[A], timeout: FiniteDuration = 500.milliseconds) = Await.result[A](f, timeout)

    val containerId = ContainerId("id")
    /**
     * Constructs a testcontainer with overridden IO methods. Results of the override can be provided
     * as parameters.
     */
    def kubernetesContainer(
        id: ContainerId = containerId,
        ip: ContainerIp = ContainerIp("ip"))(
            ccRes: Future[RunResult] = Future.successful(RunResult(intervalOf(1.millisecond), Right(ContainerResponse(true, "", None)))),
            retryCount: Int = 0)(
                implicit kubernetes: KubernetesApi): KubernetesContainer = {

        new KubernetesContainer(id, ip) {
            override protected def callContainer(path: String, body: JsObject, timeout: FiniteDuration, retry: Boolean = false): Future[RunResult] = {
                ccRes
            }
            override protected val logsRetryCount = retryCount
            override protected val logsRetryWait = 0.milliseconds
        }
    }

    /** Creates an interval starting at EPOCH with the given duration. */
    def intervalOf(duration: FiniteDuration) = Interval(Instant.EPOCH, Instant.ofEpochMilli(duration.toMillis))

    behavior of "KubernetesContainer"

    implicit val transid = TransactionId.testing

    /*
     * CONTAINER CREATION
     */
    it should "create a new instance" in {
        implicit val kubernetes = new TestKubernetesClient
        implicit val runc = stub[RuncApi]

        val image = "image"
        val labels = Map("test" -> "hi")
        val name = "myContainer"

        val container = KubernetesContainer.create(
            transid = transid,
            image = image,
            labels = labels,
            name = Some(name))

        await(container)

        kubernetes.runs should have size 1
        kubernetes.inspects should have size 1
        kubernetes.rms should have size 0

        val (testImage, testName, testLabels) = kubernetes.runs.head
        testImage shouldBe "image"
        testName shouldBe "myContainer"
        testLabels shouldBe labels
    }

    it should "pull a user provided image before creating the container" in {
        implicit val kubernetes = new TestKubernetesClient
        implicit val runc = stub[RuncApi]

        val container = KubernetesContainer.create(transid = transid, image = "image")
        await(container)

        kubernetes.runs should have size 1
        kubernetes.inspects should have size 1
        kubernetes.rms should have size 0
    }

    it should "remove the container if inspect fails" in {
        implicit val kubernetes = new TestKubernetesClient {
            override def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerIp] = {
                inspects += id
                Future.failed(new RuntimeException())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = KubernetesContainer.create(transid = transid, image = "image")
        a[WhiskContainerStartupError] should be thrownBy await(container)

        kubernetes.runs should have size 1
        kubernetes.inspects should have size 1
        kubernetes.rms should have size 1
    }

    it should "provide a proper error if run fails for blackbox containers" in {
        implicit val kubernetes = new TestKubernetesClient {
            override def run(image: String, name: String, labels: Map[String, String] = Map())(implicit transid: TransactionId): Future[ContainerId] = {
                runs += ((image, name, labels))
                Future.failed(new RuntimeException())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = KubernetesContainer.create(transid = transid, image = "image")
        a[WhiskContainerStartupError] should be thrownBy await(container)

        kubernetes.runs should have size 1
        kubernetes.inspects should have size 0
        kubernetes.rms should have size 0
    }

    it should "provide a proper error if inspect fails for blackbox containers" in {
        implicit val kubernetes = new TestKubernetesClient {
            override def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerIp] = {
                inspects += id
                Future.failed(new RuntimeException())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = KubernetesContainer.create(transid = transid, image = "image")
        a[WhiskContainerStartupError] should be thrownBy await(container)

        kubernetes.runs should have size 1
        kubernetes.inspects should have size 1
        kubernetes.rms should have size 1
    }

    /*
     * KUBERNETES COMMANDS
     */
    it should "halt and resume container via runc" in {
        implicit val kubernetes = stub[KubernetesApi]
        implicit val runc = stub[RuncApi]

        val id = ContainerId("id")
        val container = new KubernetesContainer(id, ContainerIp("ip"))

        container.suspend()
        container.resume()

        (runc.pause(_: ContainerId)(_: TransactionId)).verify(id, transid)
        (runc.resume(_: ContainerId)(_: TransactionId)).verify(id, transid)
    }

    it should "destroy a container via Kubernetes" in {
        implicit val kubernetes = stub[KubernetesApi]
        implicit val runc = stub[RuncApi]

        val id = ContainerId("id")
        val container = new KubernetesContainer(id, ContainerIp("ip"))

        container.destroy()

        (kubernetes.rm(_: ContainerId)(_: TransactionId)).verify(id, transid)
    }

    /*
     * INITIALIZE
     *
     * Only tests for quite simple cases. Disambiguation of errors is delegated to ActivationResponse
     * and so are the tests for those.
     */
    it should "initialize a container" in {
        implicit val kubernetes = stub[KubernetesApi]
        implicit val runc = stub[RuncApi]

        val interval = intervalOf(1.millisecond)
        val container = kubernetesContainer() {
            Future.successful(RunResult(interval, Right(ContainerResponse(true, "", None))))
        }

        val initInterval = container.initialize(JsObject(), 1.second)
        await(initInterval) shouldBe interval

        // assert the starting log is there
        val start = LogMarker.parse(logLines.head)
        start.token shouldBe INVOKER_ACTIVATION_INIT

        // assert the end log is there
        val end = LogMarker.parse(logLines.last)
        end.token shouldBe INVOKER_ACTIVATION_INIT.asFinish
        end.deltaToMarkerStart shouldBe Some(interval.duration.toMillis)
    }

    it should "properly deal with a timeout during initialization" in {
        implicit val kubernetes = stub[KubernetesApi]
        implicit val runc = stub[RuncApi]

        val initTimeout = 1.second
        val interval = intervalOf(initTimeout + 1.nanoseconds)

        val container = kubernetesContainer() {
            Future.successful(RunResult(interval, Left(Timeout())))
        }

        val init = container.initialize(JsObject(), initTimeout)

        val error = the[InitializationError] thrownBy await(init)
        error.interval shouldBe interval
        error.response.statusCode shouldBe ActivationResponse.ApplicationError

        // assert the finish log is there
        val end = LogMarker.parse(logLines.last)
        end.token shouldBe INVOKER_ACTIVATION_INIT.asFinish
    }

    /*
     * RUN
     *
     * Only tests for quite simple cases. Disambiguation of errors is delegated to ActivationResponse
     * and so are the tests for those.
     */
    it should "run a container" in {
        implicit val kubernetes = stub[KubernetesApi]
        implicit val runc = stub[RuncApi]

        val interval = intervalOf(1.millisecond)
        val result = JsObject()
        val container = kubernetesContainer() {
            Future.successful(RunResult(interval, Right(ContainerResponse(true, result.compactPrint, None))))
        }

        val runResult = container.run(JsObject(), JsObject(), 1.second)
        await(runResult) shouldBe (interval, ActivationResponse.success(Some(result)))

        // assert the starting log is there
        val start = LogMarker.parse(logLines.head)
        start.token shouldBe INVOKER_ACTIVATION_RUN

        // assert the end log is there
        val end = LogMarker.parse(logLines.last)
        end.token shouldBe INVOKER_ACTIVATION_RUN.asFinish
        end.deltaToMarkerStart shouldBe Some(interval.duration.toMillis)
    }

    it should "properly deal with a timeout during run" in {
        implicit val kubernetes = stub[KubernetesApi]
        implicit val runc = stub[RuncApi]

        val runTimeout = 1.second
        val interval = intervalOf(runTimeout + 1.nanoseconds)

        val container = kubernetesContainer() {
            Future.successful(RunResult(interval, Left(Timeout())))
        }

        val runResult = container.run(JsObject(), JsObject(), runTimeout)
        await(runResult) shouldBe (interval, ActivationResponse.applicationError(Messages.timedoutActivation(runTimeout, false)))

        // assert the finish log is there
        val end = LogMarker.parse(logLines.last)
        end.token shouldBe INVOKER_ACTIVATION_RUN.asFinish
    }

    /*
     * LOGS
     */
    def toByteBuffer(s: String): ByteBuffer = {
        val bb = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))
        // Set position behind provided string to simulate read - this is what FileChannel.read() does
        // Otherwise position would be 0 indicating that buffer is empty
        bb.position(bb.capacity())
        bb
    }

    def toRawLog(log: Seq[LogLine], appendSentinel: Boolean = true): String = {
        val appendedLog = if (appendSentinel) {
            val lastTime = log.lastOption.map { case LogLine(time, _, _) => time }.getOrElse(Instant.EPOCH.toString)
            log :+
                LogLine(lastTime, "stderr", s"${ActionLogDriver.LOG_ACTIVATION_SENTINEL}\n") :+
                LogLine(lastTime, "stdout", s"${ActionLogDriver.LOG_ACTIVATION_SENTINEL}\n")
        } else {
            log
        }
        appendedLog.map(_.toJson.compactPrint).mkString("", "\n", "\n")
    }

    it should "read a simple log with sentinel" in {
        val expectedLogEntry = LogLine(Instant.EPOCH.toString, "stdout", "This is a log entry.\n")
        val rawLog = toRawLog(Seq(expectedLogEntry), appendSentinel = true)
        val readResults = mutable.Queue(rawLog)

        implicit val kubernetes = new TestKubernetesClient {
            override def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
                logsInvocations += ((containerId, sinceTime))
                Future.successful(readResults.dequeue())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = kubernetesContainer(id = containerId)()
        // Read with tight limit to verify that no truncation occurs
        val processedLogs = await(container.logs(limit = expectedLogEntry.log.sizeInBytes, waitForSentinel = true))

        kubernetes.logsInvocations should have size 1
        val (id, sinceTime) = kubernetes.logsInvocations(0)
        id shouldBe containerId
        sinceTime shouldBe ""

        processedLogs should have size 1
        processedLogs shouldBe Vector(expectedLogEntry.toFormattedString)
    }

    it should "read a simple log without sentinel" in {
        val expectedLogEntry = LogLine(Instant.EPOCH.toString, "stdout", "This is a log entry.\n")
        val rawLog = toRawLog(Seq(expectedLogEntry), appendSentinel = false)
        val readResults = mutable.Queue(rawLog)

        implicit val kubernetes = new TestKubernetesClient {
            override def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
                logsInvocations += ((containerId, sinceTime))
                Future.successful(readResults.dequeue())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = kubernetesContainer(id = containerId)()
        // Read without tight limit so that the full read result is processed
        val processedLogs = await(container.logs(limit = 1.MB, waitForSentinel = false))

        kubernetes.logsInvocations should have size 1
        val (id, sinceTime) = kubernetes.logsInvocations(0)
        id shouldBe containerId
        sinceTime shouldBe ""

        processedLogs should have size 1
        processedLogs shouldBe Vector(expectedLogEntry.toFormattedString)
    }

    it should "fail log reading if error occurs during file reading" in {
        implicit val kubernetes = new TestKubernetesClient {
            override def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
                logsInvocations += ((containerId, sinceTime))
                Future.failed(new IOException)
            }
        }
        implicit val runc = stub[RuncApi]

        val container = kubernetesContainer()()
        an[IOException] should be thrownBy await(container.logs(limit = 1.MB, waitForSentinel = true))

        kubernetes.logsInvocations should have size 1
        val (id, sinceTime) = kubernetes.logsInvocations(0)
        id shouldBe containerId
        sinceTime shouldBe ""

        exactly(1, logLines) should include(s"Failed to obtain logs of ${containerId.asString}")
    }

    it should "read two consecutive logs with sentinel" in {
        val firstLogEntry = LogLine(Instant.EPOCH.toString, "stdout", "This is the first log.\n")
        val secondLogEntry = LogLine(Instant.EPOCH.plusSeconds(1L).toString, "stderr", "This is the second log.\n")
        val firstRawLog = toRawLog(Seq(firstLogEntry), appendSentinel = true)
        val secondRawLog = toRawLog(Seq(secondLogEntry), appendSentinel = true)
        val returnValues = mutable.Queue(firstRawLog, secondRawLog)

        implicit val kubernetes = new TestKubernetesClient {
            override def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
                logsInvocations += ((containerId, sinceTime))
                Future.successful(returnValues.dequeue())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = kubernetesContainer()()
        // Read without tight limit so that the full read result is processed
        val processedFirstLog = await(container.logs(limit = 1.MB, waitForSentinel = true))
        val processedSecondLog = await(container.logs(limit = 1.MB, waitForSentinel = true))

        kubernetes.logsInvocations should have size 2
        val (_, sinceTime1) = kubernetes.logsInvocations(0)
        sinceTime1 shouldBe ""
        val (_, sinceTime2) = kubernetes.logsInvocations(1)
        sinceTime2 shouldBe "" // second read should start behind the first line

        processedFirstLog should have size 1
        processedFirstLog shouldBe Vector(firstLogEntry.toFormattedString)
        processedSecondLog should have size 1
        processedSecondLog shouldBe Vector(secondLogEntry.toFormattedString)
    }

    it should "retry log reading if sentinel cannot be found in the first place" in {
        val retries = 15
        val expectedLog = (1 to retries).map { i =>
            LogLine(
                Instant.EPOCH.plusMillis(i.toLong).toString,
                "stdout",
                s"This is log entry ${i}.\n")
        }.toVector
        val returnValues = mutable.Queue.empty[String]
        for (i <- 0 to retries) {
            // Sentinel only added for the last return value
            returnValues += toRawLog(expectedLog.take(i).toSeq, appendSentinel = (i == retries))
        }

        implicit val kubernetes = new TestKubernetesClient {
            override def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
                logsInvocations += ((containerId, sinceTime))
                Future.successful(returnValues.dequeue())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = kubernetesContainer()(retryCount = retries)
        // Read without tight limit so that the full read result is processed
        val processedLog = await(container.logs(limit = 1.MB, waitForSentinel = true))

        kubernetes.logsInvocations should have size retries + 1
        forAll(kubernetes.logsInvocations) {
            case (_, sinceTime) => sinceTime shouldBe ""
        }

        processedLog should have size expectedLog.length
        processedLog shouldBe expectedLog.map(_.toFormattedString)

        (retries to 1).foreach { i => exactly(1, logLines) should include(s"log cursor advanced but missing sentinel, trying ${i} more times") }
    }

    it should "provide full log if log reading retries are exhausted and no sentinel can be found" in {
        val retries = 15
        val expectedLog = (1 to retries).map { i =>
            LogLine(
                Instant.EPOCH.plusMillis(i.toLong).toString,
                "stdout",
                s"This is log entry ${i}.\n")
        }.toVector
        val returnValues = mutable.Queue.empty[String]
        for (i <- 0 to retries) {
            returnValues += toRawLog(expectedLog.take(i).toSeq, appendSentinel = false)
        }

        implicit val kubernetes = new TestKubernetesClient {
            override def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
                logsInvocations += ((containerId, sinceTime))
                Future.successful(returnValues.dequeue())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = kubernetesContainer()(retryCount = retries)
        // Read without tight limit so that the full read result is processed
        val processedLog = await(container.logs(limit = 1.MB, waitForSentinel = true))

        kubernetes.logsInvocations should have size retries + 1
        forAll(kubernetes.logsInvocations) {
            case (_, sinceTime) => sinceTime shouldBe ""
        }

        processedLog should have size expectedLog.length
        processedLog shouldBe expectedLog.map(_.toFormattedString)

        (retries to 1).foreach { i => exactly(1, logLines) should include(s"log cursor advanced but missing sentinel, trying ${i} more times") }
    }

    it should "truncate logs and advance reading position to end of current read" in {
        val firstLogFirstEntry = LogLine(Instant.EPOCH.toString, "stdout", "This is the first line in first log.\n")
        val firstLogSecondEntry = LogLine(Instant.EPOCH.plusMillis(1L).toString, "stderr", "This is the second line in first log.\n")

        val secondLogFirstEntry = LogLine(Instant.EPOCH.plusMillis(2L).toString, "stdout", "This is the first line in second log.\n")
        val secondLogSecondEntry = LogLine(Instant.EPOCH.plusMillis(3L).toString, "stdout", "This is the second line in second log.\n")
        val secondLogLimit = 4

        val thirdLogFirstEntry = LogLine(Instant.EPOCH.plusMillis(4L).toString, "stdout", "This is the first line in third log.\n")

        val firstRawLog = toRawLog(Seq(firstLogFirstEntry, firstLogSecondEntry), appendSentinel = true)
        val secondRawLog = toRawLog(Seq(secondLogFirstEntry, secondLogSecondEntry), appendSentinel = false)
        val thirdRawLog = toRawLog(Seq(thirdLogFirstEntry), appendSentinel = true)

        val returnValues = mutable.Queue(firstRawLog, secondRawLog, thirdRawLog)

        implicit val kubernetes = new TestKubernetesClient {
            override def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
                logsInvocations += ((containerId, sinceTime))
                Future.successful(returnValues.dequeue())
            }
        }
        implicit val runc = stub[RuncApi]

        val container = kubernetesContainer()()
        val processedFirstLog = await(container.logs(limit = firstLogFirstEntry.log.sizeInBytes, waitForSentinel = true))
        val processedSecondLog = await(container.logs(limit = secondLogFirstEntry.log.take(secondLogLimit).sizeInBytes, waitForSentinel = false))
        val processedThirdLog = await(container.logs(limit = 1.MB, waitForSentinel = true))

        kubernetes.logsInvocations should have size 3
        val (_, sinceTime1) = kubernetes.logsInvocations(0)
        sinceTime1 shouldBe 0
        val (_, sinceTime2) = kubernetes.logsInvocations(1)
        sinceTime2 shouldBe "" // second read should start behind full content of first read
        val (_, sinceTime3) = kubernetes.logsInvocations(2)
        sinceTime3 shouldBe "" // third read should start behind full content of first and second read

        processedFirstLog should have size 2
        processedFirstLog(0) shouldBe firstLogFirstEntry.toFormattedString
        processedFirstLog(1) should startWith(Messages.truncateLogs(firstLogFirstEntry.log.sizeInBytes))

        processedSecondLog should have size 2
        processedSecondLog(0) shouldBe secondLogFirstEntry.copy(log = secondLogFirstEntry.log.take(secondLogLimit)).toFormattedString
        processedSecondLog(1) should startWith(Messages.truncateLogs(secondLogFirstEntry.log.take(secondLogLimit).sizeInBytes))

        processedThirdLog should have size 1
        processedThirdLog(0) shouldBe thirdLogFirstEntry.toFormattedString
    }

    class TestKubernetesClient extends KubernetesApi {
        var runs = mutable.Buffer.empty[(String, String, Map[String,String])]
        var inspects = mutable.Buffer.empty[ContainerId]
        var rms = mutable.Buffer.empty[ContainerId]
        var logsInvocations = mutable.Buffer.empty[(ContainerId, String)]

        def run(image: String, name: String, labels: Map[String, String] = Map())(implicit transid: TransactionId): Future[ContainerId] = {
            runs += ((image, name, labels))
            Future.successful(ContainerId("testId"))
        }

        def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerIp] = {
            inspects += id
            Future.successful(ContainerIp("testIp"))
        }

        def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit] = {
            rms += id
            Future.successful(())
        }

        def rm(key: String, value: String)(implicit transid: TransactionId): Future[Unit] = {
            // TODO: test
            Future.successful(())
        }

        def logs(containerId: ContainerId, sinceTime: String)(implicit transid: TransactionId): Future[String] = {
            logsInvocations += ((containerId, sinceTime))
            Future.successful("dummy log")
        }
    }
}
