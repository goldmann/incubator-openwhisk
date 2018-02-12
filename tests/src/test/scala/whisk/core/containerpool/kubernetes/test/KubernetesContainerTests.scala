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

import java.io.IOException
import java.time.{Instant, LocalDateTime, ZoneId}

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import common.TimingHelpers

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.Future
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FlatSpec
import whisk.core.containerpool.logging.{DockerToActivationLogStore, LogLine}
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers
import common.{StreamLogging, WskActorSystem}
import spray.json._
import whisk.common.LoggingMarkers._
import whisk.common.LogMarker
import whisk.common.TransactionId
import whisk.core.containerpool._
import whisk.core.containerpool.kubernetes._
import whisk.core.containerpool.docker._
import whisk.core.entity.ActivationResponse
import whisk.core.entity.ActivationResponse.ContainerResponse
import whisk.core.entity.ActivationResponse.Timeout
import whisk.core.entity.size._
import whisk.http.Messages
import whisk.core.containerpool.docker.test.DockerContainerTests._

/**
 * Unit tests for ContainerPool schedule
 */
@RunWith(classOf[JUnitRunner])
class KubernetesContainerTests
    extends FlatSpec
    with Matchers
    with MockFactory
    with StreamLogging
    with BeforeAndAfterEach
    with WskActorSystem
    with TimingHelpers {

  import KubernetesClientTests.TestKubernetesClient

  override def beforeEach() = {
    stream.reset()
  }

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  def instantDT(instant: Instant): LocalDateTime = LocalDateTime.from(instant.atZone(ZoneId.of("GMT+0")))

  val Epoch = Instant.EPOCH
  val EpochDateTime = instantDT(Epoch)

  /** Reads logs into memory and awaits them */
  def awaitLogs(source: Source[ByteString, Any], timeout: FiniteDuration = 500.milliseconds): Vector[String] =
    Await.result(source.via(DockerToActivationLogStore.toFormattedString).runWith(Sink.seq[String]), timeout).toVector

  val containerId = ContainerId("id")

  /**
   * Constructs a testcontainer with overridden IO methods. Results of the override can be provided
   * as parameters.
   */
  def kubernetesContainer(id: ContainerId = containerId, addr: ContainerAddress = ContainerAddress("ip"))(
    ccRes: Future[RunResult] =
      Future.successful(RunResult(intervalOf(1.millisecond), Right(ContainerResponse(true, "", None)))),
    awaitLogs: FiniteDuration = 2.seconds)(implicit kubernetes: KubernetesApi): KubernetesContainer = {

    new KubernetesContainer(id, addr) {
      override protected def callContainer(
        path: String,
        body: JsObject,
        timeout: FiniteDuration,
        retry: Boolean = false)(implicit transid: TransactionId): Future[RunResult] = {
        ccRes
      }
      override protected val waitForLogs = awaitLogs
    }
  }

  behavior of "KubernetesContainer"

  implicit val transid = TransactionId.testing
  val parameters = Map(
    "--cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
    "--ulimit" -> Set("nofile=1024:1024"),
    "--pids-limit" -> Set("1024"))

  /*
   * CONTAINER CREATION
   */
  it should "create a new instance" in {
    implicit val kubernetes = new TestKubernetesClient

    val image = "image"
    val userProvidedImage = false
    val environment = Map("test" -> "hi")
    val labels = Map("invoker" -> "0")
    val name = "my_Container(1)"
    val container = KubernetesContainer.create(
      transid = transid,
      image = image,
      userProvidedImage = userProvidedImage,
      environment = environment,
      labels = labels,
      name = name)

    await(container)

    kubernetes.runs should have size 1
    kubernetes.inspects should have size 1
    kubernetes.rms should have size 0

    val (testName, testImage, testArgs) = kubernetes.runs.head
    testName shouldBe "my-container1"
    testImage shouldBe image
    testArgs shouldBe Seq(
      "--generator",
      "run-pod/v1",
      "--restart",
      "Always",
      "--limits",
      "memory=256Mi",
      "--env",
      "test=hi",
      "-l",
      "invoker=0")
  }

  it should "pull a user provided image before creating the container" in {
    implicit val kubernetes = new TestKubernetesClient

    val container =
      KubernetesContainer.create(transid = transid, name = "name", image = "image", userProvidedImage = true)
    await(container)

    kubernetes.runs should have size 1
    kubernetes.inspects should have size 1
    kubernetes.rms should have size 0
  }

  it should "remove the container if inspect fails" in {
    implicit val kubernetes = new TestKubernetesClient {
      override def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress] = {
        inspects += id
        Future.failed(new RuntimeException())
      }
    }

    val container = KubernetesContainer.create(transid = transid, name = "name", image = "image")
    a[WhiskContainerStartupError] should be thrownBy await(container)

    kubernetes.runs should have size 1
    kubernetes.inspects should have size 1
    kubernetes.rms should have size 1
  }

  it should "provide a proper error if run fails for blackbox containers" in {
    implicit val kubernetes = new TestKubernetesClient {
      override def run(name: String, image: String, args: Seq[String])(
        implicit transid: TransactionId): Future[ContainerId] = {
        runs += ((name, image, args))
        Future.failed(ProcessRunningException(1, "", ""))
      }
    }

    val container =
      KubernetesContainer.create(transid = transid, name = "name", image = "image", userProvidedImage = true)
    a[WhiskContainerStartupError] should be thrownBy await(container)

    kubernetes.runs should have size 1
    kubernetes.inspects should have size 0
    kubernetes.rms should have size 0
  }

  it should "provide a proper error if inspect fails for blackbox containers" in {
    implicit val kubernetes = new TestKubernetesClient {
      override def inspectIPAddress(id: ContainerId)(implicit transid: TransactionId): Future[ContainerAddress] = {
        inspects += id
        Future.failed(new RuntimeException())
      }
    }

    val container =
      KubernetesContainer.create(transid = transid, name = "name", image = "image", userProvidedImage = true)
    a[WhiskContainerStartupError] should be thrownBy await(container)

    kubernetes.runs should have size 1
    kubernetes.inspects should have size 1
    kubernetes.rms should have size 1
  }

  /*
   * KUBERNETES COMMANDS
   */
  it should "destroy a container via Kubernetes" in {
    implicit val kubernetes = stub[KubernetesApi]

    val id = ContainerId("id")
    val container = new KubernetesContainer(id, ContainerAddress("ip"))

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

    val initTimeout = 1.second
    val interval = intervalOf(1.millisecond)
    val container = kubernetesContainer() {
      Future.successful(RunResult(interval, Right(ContainerResponse(true, "", None))))
    }

    val initInterval = container.initialize(JsObject(), initTimeout)
    await(initInterval, initTimeout) shouldBe interval

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

    val initTimeout = 1.second
    val interval = intervalOf(initTimeout + 1.nanoseconds)

    val container = kubernetesContainer() {
      Future.successful(RunResult(interval, Left(Timeout())))
    }

    val init = container.initialize(JsObject(), initTimeout)

    val error = the[InitializationError] thrownBy await(init, initTimeout)
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

    val runTimeout = 1.second
    val interval = intervalOf(runTimeout + 1.nanoseconds)

    val container = kubernetesContainer() {
      Future.successful(RunResult(interval, Left(Timeout())))
    }

    val runResult = container.run(JsObject(), JsObject(), runTimeout)
    await(runResult) shouldBe (interval, ActivationResponse.applicationError(
      Messages.timedoutActivation(runTimeout, false)))

    // assert the finish log is there
    val end = LogMarker.parse(logLines.last)
    end.token shouldBe INVOKER_ACTIVATION_RUN.asFinish
  }

  /*
   * LOGS
   */
  it should "read a simple log with sentinel" in {
    val expectedLogEntry = LogLine(currentTsp, "stdout", "This is a log entry.\n")
    val logSrc = Source.single(toRawLog(Seq(expectedLogEntry), appendSentinel = true))

    implicit val kubernetes = new TestKubernetesClient {
      override def logs(id: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((id, sinceTime))
        logSrc
      }
    }

    val container = kubernetesContainer(id = containerId)()
    // Read with tight limit to verify that no truncation occurs TODO: Need to figure out how to handle this with the Source-based kubernetes logs
    val processedLogs = awaitLogs(container.logs(limit = 4096.B, waitForSentinel = true))

    kubernetes.logCalls should have size 1
    val (id, sinceTime) = kubernetes.logCalls(0)
    id shouldBe containerId
    sinceTime shouldBe None

    processedLogs should have size 1
    processedLogs shouldBe Vector(expectedLogEntry.toFormattedString)
  }

  it should "read a simple log without sentinel" in {
    val expectedLogEntry = LogLine(currentTsp, "stdout", "This is a log entry.\n")
    val logSrc = Source.single(toRawLog(Seq(expectedLogEntry), appendSentinel = false))

    implicit val kubernetes = new TestKubernetesClient {
      override def logs(id: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((id, sinceTime))
        logSrc
      }
    }

    val container = kubernetesContainer(id = containerId)()
    // Read without tight limit so that the full read result is processed
    val processedLogs = awaitLogs(container.logs(limit = 1.MB, waitForSentinel = false))

    kubernetes.logCalls should have size 1
    val (id, sinceTime) = kubernetes.logCalls(0)
    id shouldBe containerId
    sinceTime shouldBe None

    processedLogs should have size 1
    processedLogs shouldBe Vector(expectedLogEntry.toFormattedString)
  }

  it should "fail log reading if error occurs during file reading" in {
    implicit val kubernetes = new TestKubernetesClient {
      override def logs(id: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((containerId, sinceTime))
        Source.failed(new IOException)
      }
    }

    val container = kubernetesContainer()()
    an[IOException] should be thrownBy awaitLogs(container.logs(limit = 1.MB, waitForSentinel = true))

    kubernetes.logCalls should have size 1
    val (id, sinceTime) = kubernetes.logCalls(0)
    id shouldBe containerId
    sinceTime shouldBe None
  }

  it should "read two consecutive logs with sentinel" in {
    val firstLogEntry = LogLine(Instant.EPOCH.toString, "stdout", "This is the first log.\n")
    val secondLogEntry = LogLine(Instant.EPOCH.plusSeconds(1l).toString, "stderr", "This is the second log.\n")

    val firstLogSrc = toRawLog(Seq(firstLogEntry), appendSentinel = true)
    val secondLogSrc = toRawLog(Seq(secondLogEntry), appendSentinel = true)
    val returnValues = mutable.Queue(firstLogSrc, secondLogSrc)

    implicit val kubernetes = new TestKubernetesClient {
      override def logs(id: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((id, sinceTime))
        Source.single(returnValues.dequeue())
      }
    }

    val container = kubernetesContainer()()
    // Read without tight limit so that the full read result is processed
    val processedFirstLog = awaitLogs(container.logs(limit = 1.MB, waitForSentinel = true))
    val processedSecondLog = awaitLogs(container.logs(limit = 1.MB, waitForSentinel = true))

    kubernetes.logCalls should have size 2
    val (_, sinceTime1) = kubernetes.logCalls(0)
    sinceTime1 shouldBe None
    val (_, sinceTime2) = kubernetes.logCalls(1)
    sinceTime2 shouldBe Some(EpochDateTime) // second read should start behind the first line

    processedFirstLog should have size 1
    processedFirstLog shouldBe Vector(firstLogEntry.toFormattedString)
    processedSecondLog should have size 1
    processedSecondLog shouldBe Vector(secondLogEntry.toFormattedString)

  }

  it should "eventually terminate even if no sentinels can be found" in {
    val expectedLog = Seq(LogLine(currentTsp, "stdout", s"This is log entry.\n"))
    val rawLog = toRawLog(expectedLog, appendSentinel = false)

    implicit val kubernetes = new TestKubernetesClient {
      override def logs(containerId: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((containerId, sinceTime))
        // "Fakes" an infinite source with only 1 entry
        Source.tick(0.milliseconds, 10.seconds, rawLog)
      }
    }

    val waitForLogs = 100.milliseconds
    val container = kubernetesContainer()(awaitLogs = waitForLogs)
    // Read without tight limit so that the full read result is processed

    val (interval, processedLog) = durationOf(awaitLogs(container.logs(limit = 1.MB, waitForSentinel = true)))

    interval.toMillis should (be >= waitForLogs.toMillis and be < (waitForLogs * 2).toMillis)

    kubernetes.logCalls should have size 1

    processedLog should have size expectedLog.length
    processedLog shouldBe expectedLog.map(_.toFormattedString)
  }

  it should "not fail if the last log-line is incomplete" in {
    val expectedLogEntry =
      LogLine(currentTsp, "stdout", "This is a log entry.\n")
    // "destroy" the second log entry by dropping some bytes
    val rawLog = toRawLog(Seq(expectedLogEntry, expectedLogEntry), appendSentinel = false).dropRight(10)

    implicit val kubernetes = new TestKubernetesClient {
      override def logs(containerId: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((containerId, sinceTime))
        Source.single(rawLog)
      }
    }

    val container = kubernetesContainer(id = containerId)()
    // Read with tight limit to verify that no truncation occurs
    val processedLogs = awaitLogs(container.logs(limit = rawLog.length.bytes, waitForSentinel = false))

    kubernetes.logCalls should have size 1
    val (id, sinceTime) = kubernetes.logCalls(0)
    id shouldBe containerId
    sinceTime shouldBe None

    processedLogs should have size 2
    processedLogs(0) shouldBe expectedLogEntry.toFormattedString
    processedLogs(1) should include(Messages.logFailure)
  }

  it should "include an incomplete warning if sentinels have not been found only if we wait for sentinels" in {
    val expectedLogEntry =
      LogLine(currentTsp, "stdout", "This is a log entry.\n")
    val rawLog = Source.single(toRawLog(Seq(expectedLogEntry, expectedLogEntry), appendSentinel = false))

    implicit val kubernetes = new TestKubernetesClient {
      override def logs(containerId: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((containerId, sinceTime))
        rawLog
      }
    }

    val waitForLogs = 100.milliseconds
    val container = kubernetesContainer()(awaitLogs = waitForLogs)
    // Read with tight limit to verify that no truncation occurs
    val processedLogs = awaitLogs(container.logs(limit = 4096.B, waitForSentinel = true))

    kubernetes.logCalls should have size 1
    val (id, sinceTime) = kubernetes.logCalls(0)
    id shouldBe containerId
    sinceTime shouldBe None

    processedLogs should have size 3
    processedLogs(0) shouldBe expectedLogEntry.toFormattedString
    processedLogs(1) shouldBe expectedLogEntry.toFormattedString
    processedLogs(2) should include(Messages.logFailure)

    val processedLogsFalse = awaitLogs(container.logs(limit = 4096.B, waitForSentinel = false))
    processedLogsFalse should have size 2
    processedLogsFalse(0) shouldBe expectedLogEntry.toFormattedString
    processedLogsFalse(1) shouldBe expectedLogEntry.toFormattedString
  }

  it should "strip sentinel lines if it waits or doesn't wait for them" in {
    val expectedLogEntry =
      LogLine(currentTsp, "stdout", "This is a log entry.\n")
    val rawLog = toRawLog(Seq(expectedLogEntry), appendSentinel = true)

    implicit val kubernetes = new TestKubernetesClient {
      override def logs(containerId: ContainerId, sinceTime: Option[LocalDateTime], waitForSentinel: Boolean)(
        implicit transid: TransactionId): Source[ByteString, Any] = {
        logCalls += ((containerId, sinceTime))
        Source.single(rawLog)
      }
    }

    val container = kubernetesContainer(id = containerId)()
    val processedLogs = awaitLogs(container.logs(limit = 1.MB, waitForSentinel = true))
    processedLogs should have size 1
    processedLogs(0) shouldBe expectedLogEntry.toFormattedString

    val processedLogsFalse = awaitLogs(container.logs(limit = 1.MB, waitForSentinel = false))
    processedLogsFalse should have size 1
    processedLogsFalse(0) shouldBe expectedLogEntry.toFormattedString
  }

  def currentTsp: String = {
    LocalDateTime.now().format(KubernetesClient.K8SDateTimeFormat)
  }

}
