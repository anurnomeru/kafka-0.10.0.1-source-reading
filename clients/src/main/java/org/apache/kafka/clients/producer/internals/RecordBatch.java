/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);

    public int recordCount = 0;// record个数（在recordBatch中的相对偏移量）

    public int maxRecordSize = 0;// 最大的那个record的字节数

    public volatile int attempts = 0;// 尝试发送次数

    public final long createdMs;// 创建时间

    public long drainedMs;//

    public long lastAttemptMs;// 最后一次尝试的时间

    public final MemoryRecords records;// 储存数据用

    public final TopicPartition topicPartition;// 消息要发往的分区

    public final ProduceRequestResult produceFuture;// 标识本类状态的future对象

    public long lastAppendTime;// 最后一次追加消息的时间

    private final List<Thunk> thunks;// ？？？

    private long offsetCounter = 0L;

    private boolean retry;// 是否正在重试

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     *
     * 将消息append 到最近的消息set中，并返回该record相对的offset，返回null代表没空间了
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        if (!this.records.hasRoomFor(key, value)) {
            return null;
        } else {
            // 将消息与offset添加到buffer中，返回crc校验值
            long checksum = this.records.append(offsetCounter++, timestamp, key, value);

            // TODO：为什么要这样？
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            this.lastAppendTime = now;

            // FutureRecordMetadata主要包含了 RecordBatch里的 ProduceRequestResult
            FutureRecordMetadata future = new FutureRecordMetadata(
                this.produceFuture,
                this.recordCount,
                timestamp,
                checksum,
                key == null ? -1 : key.length,
                value == null ? -1 : value.length);

            if (callback != null) {
                thunks.add(new Thunk(callback, future));
            }
            this.recordCount++;
            return future;
        }
    }

    /**
     * Complete the request
     *
     * 请求完成：正常响应、超时、或关闭生产者时调用这个
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param timestamp The timestamp returned by the broker.
     * @param exception The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long timestamp, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
            topicPartition,
            baseOffset,
            exception);
        // execute callbacks
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                Thunk thunk = this.thunks.get(i);
                if (exception == null) {
                    // If the timestamp returned by server is NoTimestamp, that means CreateTime is used. Otherwise LogAppendTime is used.
                    RecordMetadata metadata = new RecordMetadata(this.topicPartition, baseOffset, thunk.future.relativeOffset(),
                        timestamp == Record.NO_TIMESTAMP ? thunk.future.timestamp() : timestamp,
                        thunk.future.checksum(),
                        thunk.future.serializedKeySize(),
                        thunk.future.serializedValueSize());
                    thunk.callback.onCompletion(metadata, null);
                } else {
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition {}:", topicPartition, e);
            }
        }
        this.produceFuture.done(topicPartition, baseOffset, exception);
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {

        final Callback callback;

        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     * <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     * <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        boolean expire = false;

        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime)) {
            expire = true;
        } else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs))) {
            expire = true;
        } else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs))) {
            expire = true;
        }

        if (expire) {
            this.records.close();
            this.done(-1L, Record.NO_TIMESTAMP,
                new TimeoutException("Batch containing " + recordCount + " record(s) expired due to timeout while requesting metadata from brokers for " + topicPartition));
        }

        return expire;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }
}
