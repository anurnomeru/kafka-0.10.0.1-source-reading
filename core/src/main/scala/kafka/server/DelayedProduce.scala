/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server


import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Meter
import kafka.cluster.Partition
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

/**
  * 关心的对象是TopicPartitionOperationKey，表示某个Topic中的某个分区。
  * 假设现在有一个ProducerRequest请求，它要向名为test的Topic中追加消息，分区的编号为0，此分区当前的ISR集合中有三个副本，
  * 该ProducerRequest的acks字段为-1，表示需要ISR集合中所有副本都同步了该请求中的消息才能返回DelayedProduce对象，
  * 并交给DelayedOperationPurgatory管理，DOP会将其存放到“test-0”（TopicPartitionOperationKey对象）对应的watchers中。
  * 同时也提交给时间轮。
  *
  * 之后每当Leader副本收到Follower副本发送的对“test-0”的FetchRequest时，都会检测“test-0”对应的Watchers中的DelayedProduce
  * 是否已经满足了执行条件，如果满足执行条件就会执行DelayedProduce，向客户端返回ProduceResponse。
  *
  * 如果满足执行条件就会执行DelayedProduce，向客户端返回ProduceResponse，
  * 最终，该DelayedProduce 会因满足执行条件或时间到期而被执行。
  *
  */
case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false // 在有错误的情况下，将acksPending设置为false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.errorCode, responseStatus.baseOffset, requiredOffset)
}

/**
  * The produce metadata maintained by the delayed produce operation
  *
  * 通过delayed produce 操作来维护produce的metadata
  *
  * 里面其实就是一个需不需要ack
  * 一个Map，维护TP和TP状态
  */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
  * A delayed produce operation that can be created by the replica manager and watched
  * in the produce operation purgatory
  *
  * 一个延迟的生产操作可以被replica manager所管理，并且被 produce operation purgatory所监控
  */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata, // 为一个ProducerRequest中的所有相关分区记录了一些追加消息后的返回结果，主要用于判断DelayedProduce是否满足执行条件
                     // 主要用于判断DelayedProduce是否满足执行条件
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit)
  extends DelayedOperation(delayMs) {

  // first update the acks pending variable according to the error code
  // 首先 根据错误码来更新 ack 追加的值
  produceMetadata
    .produceStatus // Map[TopicPartition, ProducePartitionStatus]
    .foreach { case (topicPartition, status) =>
    if (status.responseStatus.errorCode == Errors.NONE.code) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      status.responseStatus.errorCode = Errors.REQUEST_TIMED_OUT.code
    } else {

      // 在有错误的情况下，将acksPending设置为false
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
    * The delayed produce operation can be completed if every partition
    * it produces to is satisfied by one of the following:
    *
    * Case A: This broker is no longer the leader: set an error in response
    * Case B: This broker is the leader:
    *   B.1 - If there was a local error thrown while checking if at least requiredAcks
    * replicas have caught up to this operation: set an error in response
    *   B.2 - Otherwise, set the response with no error.
    *
    */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    produceMetadata
      .produceStatus
      .foreach { case (topicAndPartition, status) /* TopicPartition, ProducePartitionStatus */ =>
        trace("Checking produce satisfaction for %s, current status %s"
          .format(topicAndPartition, status))
        // skip those partitions that have already been satisfied
        if (status.acksPending) {
          val partitionOpt: Option[Partition] = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
          val (hasEnough, errorCode) = partitionOpt match {
            case Some(partition) =>
              partition.checkEnoughReplicasReachOffset(status.requiredOffset)
            case None =>
              // Case A  ====  This broker is no longer the leader: set an error in response
              (false, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
          }
          if (errorCode != Errors.NONE.code) {
            // Case B.1
            status.acksPending = false
            status.responseStatus.errorCode = errorCode
          } else if (hasEnough) {
            // Case B.2
            status.acksPending = false
            status.responseStatus.errorCode = Errors.NONE.code
          }
        }
      }

    // check if each partition has satisfied at lease one of case A and case B
    if (!produceMetadata.produceStatus.values.exists(p => p.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
    * Upon completion, return the current response status along with the error code per partition
    */
  override def onComplete() {
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
      "requests",
      TimeUnit.SECONDS,
      tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

