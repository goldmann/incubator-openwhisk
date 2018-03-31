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

import common.StreamLogging
import common.WskActorSystem
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.containerpool.docker.test.DockerContainerTests.await
import whisk.core.containerpool.kubernetes.KubernetesContainerFactory
import whisk.core.containerpool.kubernetes.test.KubernetesClientTests.TestKubernetesClient
import whisk.core.entity.ExecManifest.ImageName
import whisk.core.invoker.Invoker
import whisk.core.entity.size._
import whisk.core.containerpool.kubernetes._

/**
 * Unit tests for KubernetesContainerFactory
 */
@RunWith(classOf[JUnitRunner])
class KubernetesContainerFactoryTests
    extends FlatSpec
    with Matchers
    with MockFactory
    with StreamLogging
    with WskActorSystem {

  behavior of "KubernetesContainerFactory"

  implicit val config = new WhiskConfig(Invoker.requiredProperties)
  val invokerLabel = "invoker0"

  def containerFactory(testKubernetes: TestKubernetesClient): KubernetesContainerFactory = {
    new KubernetesContainerFactory(invokerLabel, config) {
      override implicit lazy val kubernetes = testKubernetes
    }
  }

  it should "label containers with invoker label" in {
    val kubernetes = new TestKubernetesClient
    val container = containerFactory(kubernetes).createContainer(
      tid = TransactionId.invokerWarmup,
      name = "name",
      actionImage = new ImageName("image"),
      userProvidedImage = false,
      memory = 256.MB)

    await(container)

    kubernetes.runs should have size 1
    kubernetes.rms should have size 0

    val (_, _, _, testLabels) = kubernetes.runs.head
    testLabels should contain key "invoker"
    testLabels("invoker") shouldBe invokerLabel
  }

  it should "label containers with image name" in {
    val kubernetes = new TestKubernetesClient
    val imageName = "mySpecialImage"
    val container = containerFactory(kubernetes).createContainer(
      tid = TransactionId.invokerWarmup,
      name = "name",
      actionImage = new ImageName(imageName, Option("foo"), Option("1.2.3")),
      userProvidedImage = false,
      memory = 256.MB)

    await(container)

    kubernetes.runs should have size 1
    kubernetes.rms should have size 0

    val (_, _, _, testLabels) = kubernetes.runs.head
    testLabels should contain key "image"
    testLabels("image") shouldBe imageName
  }

  it should "label prewarmed containers with status prewarmed" in {
    val kubernetes = new TestKubernetesClient
    val container = containerFactory(kubernetes).createContainer(
      tid = TransactionId.invokerWarmup,
      name = "name",
      actionImage = new ImageName("image"),
      userProvidedImage = false,
      memory = 256.MB)

    await(container)

    kubernetes.runs should have size 1
    kubernetes.rms should have size 0

    val (_, _, _, testLabels) = kubernetes.runs.head
    testLabels should contain key "status"
    testLabels("status") shouldBe KubernetesContainer.StatusPrewarmed
  }

  it should "label regular containers with status active" in {
    val kubernetes = new TestKubernetesClient
    val container = containerFactory(kubernetes).createContainer(
      tid = TransactionId.testing,
      name = "name",
      actionImage = new ImageName("image"),
      userProvidedImage = false,
      memory = 256.MB)

    await(container)

    kubernetes.runs should have size 1
    kubernetes.rms should have size 0

    val (_, _, _, testLabels) = kubernetes.runs.head
    testLabels should contain key "status"
    testLabels("status") shouldBe KubernetesContainer.StatusActive
  }

}
