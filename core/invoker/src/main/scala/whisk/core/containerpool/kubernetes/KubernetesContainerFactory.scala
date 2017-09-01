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

import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerFactory
import whisk.core.containerpool.ContainerFactoryProvider
import whisk.core.entity.ByteSize
import whisk.core.entity.ExecManifest.ImageName
import whisk.core.entity.InstanceId
import whisk.core.WhiskConfig


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
            environment = Map("__OW_API_HOST" -> config.wskApiHost),
            labels = Map("invoker" -> label),
            name = Some(name))
    }
}

object KubernetesContainerFactoryProvider extends ContainerFactoryProvider {
    override def getContainerFactory(instance: InstanceId, actorSystem: ActorSystem, logging: Logging, config: WhiskConfig): ContainerFactory =
        new KubernetesContainerFactory(s"invoker${instance.toInt}", config)(actorSystem.dispatcher, logging)
}
