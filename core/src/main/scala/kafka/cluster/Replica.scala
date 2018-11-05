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

package kafka.cluster

import java.util.concurrent.atomic.AtomicLong

import kafka.common.KafkaException
import kafka.log.Log
import kafka.server.{LogOffsetMetadata, LogReadResult}
import kafka.utils.{Logging, SystemTime, Time}

class Replica(val brokerId: Int,
              val partition: Partition,// 这个副本的分区
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None) extends Logging {
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  // HW
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  // 对于本地副本来说这就是记录追加到Log中最新消息的offset，对远程副本来说需要发送请求来获取这个值
  @volatile private[this] var logEndOffsetMetadata: LogOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata

  val topic: String = partition.topic
  val partitionId: Int = partition.partitionId

  def isLocal: Boolean = {
    log match {
      case Some(l) => true
      case None => false
    }
  }

  private[this] val lastCaughtUpTimeMsUnderlying = new AtomicLong(time.milliseconds)// 最后一次追上leader的时间戳

  def lastCaughtUpTimeMs = lastCaughtUpTimeMsUnderlying.get()

  def updateLogReadResult(logReadResult: LogReadResult) {
    logEndOffset = logReadResult.info.fetchOffsetMetadata// 更新一波LEO

    /* If the request read up to the log end offset snapshot when the read was initiated,
     * set the lastCaughtUpTimeMsUnderlying to the current time.
     * This means that the replica is fully caught up.
     *
     * 如果当请求读取初始化时，赶上了LEO快照
     * 将它的lastCaughtUpTimeMsUnderlying设置为当前时间
     * 这意味着副本赶上了进度
     */
    if (logReadResult.isReadFromLogEnd) {
      lastCaughtUpTimeMsUnderlying.set(time.milliseconds)
    }
  }

  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      // 不能直接更新本地副本的LEO TODO 由 log.get.logEndOffsetMetadata 获取
      throw new KafkaException("Should not set log end offset on partition [%s,%d]'s local replica %d".format(topic, partitionId, brokerId))
    } else {

      // 通过请求进行更新
      logEndOffsetMetadata = newLogEndOffset
      trace("Setting log end offset for replica %d for partition [%s,%d] to [%s]"
        .format(brokerId, topic, partitionId, logEndOffsetMetadata))
    }
  }

  def logEndOffset =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {// 只有本地副本可以更新HW
      highWatermarkMetadata = newHighWatermark
      trace("Setting high watermark for replica %d partition [%s,%d] on broker %d to [%s]"
        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
    } else {
      throw new KafkaException("Should not set high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  def highWatermark = highWatermarkMetadata

  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      throw new KafkaException("Should not construct complete high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  override def equals(that: Any): Boolean = {
    if (!(that.isInstanceOf[Replica]))
      return false
    val other = that.asInstanceOf[Replica]
    if (topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17 * brokerId + partition.hashCode()
  }


  override def toString(): String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if (isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
