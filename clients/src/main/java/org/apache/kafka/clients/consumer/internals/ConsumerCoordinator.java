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
package org.apache.kafka.clients.consumer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.TopicConstants;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the coordination process with the consumer coordinator.
 *
 * 这个类管理着consumer 协调器的协调过程
 */
public final class ConsumerCoordinator extends AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    /** PartitionAssignor列表 */
    private final List<PartitionAssignor> assignors;

    /** 元数据 */
    private final Metadata metadata;

    private final ConsumerCoordinatorMetrics sensors;

    /** subscriptionState对象 */
    private final SubscriptionState subscriptions;

    /** 提交offset回调 */
    private final OffsetCommitCallback defaultOffsetCommitCallback;

    /** 是否开启了自动提交 */
    private final boolean autoCommitEnabled;

    /** 自动提交的定时任务 */
    private final AutoCommitTask autoCommitTask;

    /** ????? */
    private final ConsumerInterceptors<?, ?> interceptors;

    /** 是否排除内部Topic */
    private final boolean excludeInternalTopics;

    /**
     * 储存元数据快照，主要用来检测Topic是否发生了分区数量的变化。在ConsumerCoordinator的构造方法中，
     * 会为元数据添加一个监听器，当元数据更新时，会做：
     *
     * 1、如果是AUTO_PATTERN模式，则使用用户自定义的正则表达式过滤Topic，得到需要订阅的Topic集合后，设置到
     * SubscriptionState的subscription集合和groupSubscription集合中
     *
     * 2、如果是AUTO_PATTERN或AUTO_TOPICS模式，为当前Metadata做一个快照，这个快照底层是使用HashMap记录每个Topic中的Partition的个数
     * 将新旧快照进行比较，发生变化的话，则表示消费者订阅的Topic发生分区数量变化，则讲SubscriptionState的
     * needsPartitionAssignment字段为true，需要重新进行分区分配。
     *
     * 使用metadataSnapshot字段记录变化后的新快照
     */
    private MetadataSnapshot metadataSnapshot;

    /**
     * 也是用来储存Metadata的快照信息，不过是用来检测Partition分配的过程中有没有发生分区数量变化。具体是在Leader消费者开始分区
     * 分配操作前，使用此字段记录metadata快照；收到SyncGroupResponse后，会比较此字段记录的快照与当前MetaData是否发生变化
     * 如果发生变化，则要重新进行分区分配
     */
    private MetadataSnapshot assignmentSnapshot;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(ConsumerNetworkClient client,
        String groupId,
        int sessionTimeoutMs,
        int heartbeatIntervalMs,
        List<PartitionAssignor> assignors,
        Metadata metadata,
        SubscriptionState subscriptions,
        Metrics metrics,
        String metricGrpPrefix,
        Time time,
        long retryBackoffMs,
        OffsetCommitCallback defaultOffsetCommitCallback,
        boolean autoCommitEnabled,
        long autoCommitIntervalMs,
        ConsumerInterceptors<?, ?> interceptors,
        boolean excludeInternalTopics) {
        super(client,
            groupId,
            sessionTimeoutMs,
            heartbeatIntervalMs,
            metrics,
            metricGrpPrefix,
            time,
            retryBackoffMs);
        this.metadata = metadata;

        this.metadata.requestUpdate();
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = defaultOffsetCommitCallback;
        this.autoCommitEnabled = autoCommitEnabled;
        this.assignors = assignors;

        addMetadataListener();

        if (autoCommitEnabled) {
            this.autoCommitTask = new AutoCommitTask(autoCommitIntervalMs);
            this.autoCommitTask.reschedule();
        } else {
            this.autoCommitTask = null;
        }

        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.excludeInternalTopics = excludeInternalTopics;
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            Subscription subscription = assignor.subscription(subscriptions.subscription());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    /** 使用AUTO_PATTERN模式， */
    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {

            @Override
            public void onMetadataUpdate(Cluster cluster) {

                // AUTO_PATTERN 模式的处理
                if (subscriptions.hasPatternSubscription()) {

                    Set<String> unauthorizedTopics = new HashSet<String>();
                    for (String topic : cluster.unauthorizedTopics()) {
                        if (filterTopic(topic)) {
                            unauthorizedTopics.add(topic);
                        }
                    }
                    if (!unauthorizedTopics.isEmpty()) {
                        throw new TopicAuthorizationException(unauthorizedTopics);
                    }

                    final List<String> topicsToSubscribe = new ArrayList<>();

                    for (String topic : cluster.topics())
                        if (filterTopic(topic)) {// 通过subscribedPattern 匹配Topic
                            topicsToSubscribe.add(topic);
                        }

                    // 更新 subscriptions、groupSubscription、assignment
                    subscriptions.changeSubscription(topicsToSubscribe);
                    // 更新元数据需要记录的Topic集合
                    metadata.setTopics(subscriptions.groupSubscription());
                } else if (!cluster.unauthorizedTopics()
                                   .isEmpty()) {
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
                }

                // 检查是否为AUTO_PATTERN 或AUTO_TOPICS模式
                // check if there are any changes to the metadata which should trigger a rebalance
                if (subscriptions.partitionsAutoAssigned()) {
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    if (!snapshot.equals(metadataSnapshot)) {
                        metadataSnapshot = snapshot;
                        subscriptions.needReassignment();
                    }
                }
            }
        });
    }

    private boolean filterTopic(String topic) {
        return subscriptions.getSubscribedPattern()
                            .matcher(topic)
                            .matches() &&
            !(excludeInternalTopics && TopicConstants.INTERNAL_TOPICS.contains(topic));
    }

    private PartitionAssignor lookupAssignor(String name) {
        for (PartitionAssignor assignor : this.assignors) {
            if (assignor.name()
                        .equals(name)) {
                return assignor;
            }
        }
        return null;
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
        String assignmentStrategy,
        Map<String/* memberId */, ByteBuffer/* 包含它关注了那些 topic */> allSubscriptions) {

        // leader会根据分配策略来获取assignor
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null) {
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        }

        Set<String> allSubscribedTopics = new HashSet<>();// 所有订阅的主题
        Map<String/* memberId */, Subscription/* 包含它关注了那些 topic */> subscriptions = new HashMap<>();

        /**
         * 这里是将byteBuffer反序列化出来，变成 Map<String, Subscription>
         */
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());// 反序列化一下
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen

        // leader将会开始监控组内订阅topics的改变，来确保所有的metadata变化都最终将被发现
        this.subscriptions.groupSubscribe(allSubscribedTopics);// 所有topic都扔进groupSubscription
        metadata.setTopics(this.subscriptions.groupSubscription());// 同时更新一下元数据里面的订阅信息

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        // 更新metadata并且保持跟踪元数据来进行分配，所以我们在rebalance后检查一下有没有什么东西改变了
        client.ensureFreshMetadata();
        assignmentSnapshot = metadataSnapshot;// 分配的快照等同于现在元数据的快照

        log.debug("Performing assignment for group {} using strategy {} with subscriptions {}",
            groupId, assignor.name(), subscriptions);

        // todo 核心Rebalance后，topic下分区的分配方法
        Map<String/* memberId */, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions/* allSubscriptions */);

        log.debug("Finished assignment for group {}: {}", groupId, assignment);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    @Override
    protected void onJoinComplete(int generation,
        String memberId,
        String assignmentStrategy,
        ByteBuffer assignmentBuffer) {
        // if we were the assignor, then we need to make sure that there have been no metadata updates
        // since the rebalance begin. Otherwise, we won't rebalance again until the next metadata change
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot)) {
            subscriptions.needReassignment();
            return;
        }

        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null) {
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        }

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
        // 标记一下，用于更新最后一次offsets
        subscriptions.needRefreshCommits();

        // update partition assignment
        // 更新一下分区分配情况
        subscriptions.assignFromSubscribed(assignment.partitions());

        // give the assignor a chance to update internal state based on the received assignment
        // 给予分配器一个机会来基于收到的分配情况来更新内部状态
        assignor.onAssignment(assignment);

        // reschedule the auto commit starting from now
        if (autoCommitEnabled) {
            autoCommitTask.reschedule();
        }

        // execute the user's callback after rebalance
        // 执行用户的基于Rebalance后的回调
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition assignment",
                listener.getClass()
                        .getName(), groupId, e);
        }
    }

    /**
     * 1、如果开启了自动提交offset则会进行同步提交offset（可能阻塞）
     * 2、调用 Subscription 中ConsumerRebalance中ConsumerRebalance中的回调方法
     * 3、设置needsPartitionAssignment 为true，收缩groupSubscription集合
     */
    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
        // 在 auto-commit 开启的情况下，commit offsets 比 rebalance 优先
        maybeAutoCommitOffsetsSync();// 进行一次同步提交offset

        // execute the user's callback before rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition revocation",
                listener.getClass()
                        .getName(), groupId, e);
        }

        assignmentSnapshot = null;
        subscriptions.needReassignment();
    }

    @Override
    public boolean needRejoin() {
        return subscriptions.partitionsAutoAssigned() &&
            (super.needRejoin() || subscriptions.partitionAssignmentNeeded());
    }

    /**
     * Refresh the committed offsets for provided partitions.
     * 为所提供的partitions刷新committed offsets
     */
    public void refreshCommittedOffsetsIfNeeded() {
        if (subscriptions.refreshCommitsNeeded()) {
            // 发送offsetFetch】Request，并处理OffsetFetchResponse响应，返回最近提交的offset集合
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp)) {
                    this.subscriptions.committed(tp, entry.getValue());
                }
            }
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     *
     * @param partitions The partitions to fetch offsets for
     *
     * @return A map from partition to the committed offset
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            ensureCoordinatorReady();

            // contact coordinator to fetch committed offsets
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            client.poll(future);

            if (future.succeeded()) {
                return future.value();
            }

            if (!future.isRetriable()) {
                throw future.exception();
            }

            time.sleep(retryBackoffMs);
        }
    }

    /**
     * Ensure that we have a valid partition assignment from the coordinator.
     * 确保我们从Coordinator获取了拥有有效的partition分配
     */
    public void ensurePartitionAssignment() {
        if (subscriptions.partitionsAutoAssigned()) {// SubscriptionType.AUTO_TOPICS 或 SubscriptionType.AUTO_PATTERN;

            // Due to a race condition between the initial metadata fetch and the initial rebalance, we need to ensure that
            // the metadata is fresh before joining initially, and then request the metadata update. If metadata update arrives
            // while the rebalance is still pending (for example, when the join group is still inflight), then we will lose
            // track of the fact that we need to rebalance again to reflect the change to the topic subscription. Without
            // ensuring that the metadata is fresh, any metadata update that changes the topic subscriptions and arrives with a
            // rebalance in progress will essentially be ignored. See KAFKA-3949 for the complete description of the problem.

            // 基于初始化获取metadata与初始化Rebalance的竞态条件，我们需要确保在进行初始化时，metadata是新的，然后
            // 请求metadata的更新。如果在rebalance还在等待时metadata的更新抵达了（比如，加入group的操作依旧inflight），
            // 我们将会丢失真实的踪迹？，所以我们需要再次rebalance来反应topic订阅情况的改变。在没有确保metadata是新鲜的之前，
            // 任何改变topic订阅以及在rebalance时到来的metadata更新将会被忽略。完整表述这个问题，可以看看 KAFKA-3949

            if (subscriptions.hasPatternSubscription()) {// SubscriptionType.AUTO_PATTERN
                client.ensureFreshMetadata();// 防止因为使用过期的Metadata进行Rebalance而导致多次连续的Rebalance
            }

            ensureActiveGroup();
        }
    }

    @Override
    public void close() {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync();
        } finally {
            super.close();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        this.subscriptions.needRefreshCommits();
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {

            @Override
            public void onSuccess(Void value) {
                if (interceptors != null) {
                    interceptors.onCommit(offsets);
                }
                cb.onComplete(offsets, null);
            }

            @Override
            public void onFailure(RuntimeException e) {
                if (e instanceof RetriableException) {
                    cb.onComplete(offsets, new RetriableCommitFailedException("Commit offsets failed with retriable exception. You should retry committing offsets.", e));
                } else {
                    cb.onComplete(offsets, e);
                }
            }
        });

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.

        // 确保提交有机会被传输（不在它完成时阻塞）？
        // 注意提交会被当作heartbeats来对待，所以这里不需要通过延迟任务执行显示地允许心跳
        client.pollNoWakeup();
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     *
     * @param offsets The offsets to be committed
     *
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     * or to any of the specified partitions
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     */
    public void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty()) {
            return;
        }

        while (true) {
            ensureCoordinatorReady();

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future);

            if (future.succeeded()) {
                if (interceptors != null) {
                    interceptors.onCommit(offsets);
                }
                return;
            }

            if (!future.isRetriable()) {
                throw future.exception();
            }

            time.sleep(retryBackoffMs);
        }
    }

    private class AutoCommitTask implements DelayedTask {

        private final long interval;

        public AutoCommitTask(long interval) {
            this.interval = interval;
        }

        private void reschedule() {
            client.schedule(this, time.milliseconds() + interval);
        }

        private void reschedule(long at) {
            client.schedule(this, at);
        }

        public void run(final long now) {
            if (coordinatorUnknown()) {
                log.debug("Cannot auto-commit offsets for group {} since the coordinator is unknown", groupId);
                reschedule(now + retryBackoffMs);
                return;
            }

            if (needRejoin()) {
                // skip the commit when we're rejoining since we'll commit offsets synchronously
                // before the revocation callback is invoked
                reschedule(now + interval);
                return;
            }

            commitOffsetsAsync(subscriptions.allConsumed(), new OffsetCommitCallback() {

                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception == null) {
                        reschedule(now + interval);
                    } else {
                        log.warn("Auto offset commit failed for group {}: {}", groupId, exception.getMessage());
                        reschedule(now + interval);
                    }
                }
            });
        }
    }

    private void maybeAutoCommitOffsetsSync() {
        if (autoCommitEnabled) {// 如果开启了自动提交
            try {
                commitOffsetsSync(subscriptions.allConsumed());
            } catch (WakeupException e) {
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Auto offset commit failed for group {}: {}", groupId, e.getMessage());
            }
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     *
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (coordinatorUnknown()) {
            return RequestFuture.coordinatorNotAvailable();
        }

        if (offsets.isEmpty()) {
            return RequestFuture.voidSuccess();
        }

        // create the offset commit request
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());

        // 有点搞不明白这里的意义在哪里？？？版本兼容？？ OffsetAndMetadata 和 PartitionData 的数据结构完全一样
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        OffsetCommitRequest req = new OffsetCommitRequest(this.groupId,
            this.generation,
            this.memberId,
            OffsetCommitRequest.DEFAULT_RETENTION_TIME,
            offsetData);

        log.trace("Sending offset-commit request with {} to coordinator {} for group {}", offsets, coordinator, groupId);

        return client.send(coordinator, ApiKeys.OFFSET_COMMIT, req)
                     .compose(new OffsetCommitResponseHandler(offsets));
    }

    public static class DefaultOffsetCommitCallback implements OffsetCommitCallback {

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null) {
                log.error("Offset commit failed.", exception);
            }
        }
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public OffsetCommitResponse parse(ClientResponse response) {
            return new OffsetCommitResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData()
                                                                        .entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                Errors error = Errors.forCode(entry.getValue());
                if (error == Errors.NONE) {
                    log.debug("Group {} committed offset {} for partition {}", groupId, offset, tp);
                    if (subscriptions.isAssigned(tp))
                    // update the local cache only if the partition is still assigned
                    {
                        subscriptions.committed(tp, offsetAndMetadata);
                    }
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    log.error("Not authorized to commit offsets for group {}", groupId);
                    future.raise(new GroupAuthorizationException(groupId));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(tp.topic());
                } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                    || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                    // raise the error to the user
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP
                    || error == Errors.REQUEST_TIMED_OUT) {
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    coordinatorDead();
                    future.raise(error);
                    return;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                    || error == Errors.ILLEGAL_GENERATION
                    || error == Errors.REBALANCE_IN_PROGRESS) {
                    // need to re-join group
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    subscriptions.needReassignment();
                    future.raise(new CommitFailedException("Commit cannot be completed since the group has already " +
                        "rebalanced and assigned the partitions to another member. This means that the time " +
                        "between subsequent calls to poll() was longer than the configured session.timeout.ms, " +
                        "which typically implies that the poll loop is spending too much time message processing. " +
                        "You can address this either by increasing the session timeout or by reducing the maximum " +
                        "size of batches returned in poll() with max.poll.records."));
                    return;
                } else {
                    log.error("Group {} failed to commit partition {} at offset {}: {}", groupId, tp, offset, error.message());
                    future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                    return;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {} for group {}", unauthorizedTopics, groupId);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     *
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        if (coordinatorUnknown()) {
            return RequestFuture.coordinatorNotAvailable();
        }

        log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
        // construct the request
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, ApiKeys.OFFSET_FETCH, request)
                     .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {

        @Override
        public OffsetFetchResponse parse(ClientResponse response) {
            return new OffsetFetchResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData()
                                                                                   .size());

            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData()
                                                                                              .entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = Errors.forCode(data.errorCode);
                    log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());

                    if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                        // just retry
                        future.raise(error);
                    } else if (error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(error);
                    } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                        // need to re-join group
                        subscriptions.needReassignment();
                        future.raise(error);
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Group {} has no committed offset for partition {}", groupId, tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {

        public final Metrics metrics;

        public final String metricGrpName;

        public final Sensor commitLatency;

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(metrics.metricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second"), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {

                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions()
                                            .size();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {

        private final Map<String, Integer> partitionsPerTopic;

        public MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }
}
