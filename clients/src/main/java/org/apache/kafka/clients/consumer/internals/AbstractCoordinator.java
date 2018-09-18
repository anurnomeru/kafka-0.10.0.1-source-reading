/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
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
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * AbstractCoordinator通过与一个指定的 kafkaBroker（Coordinator）交互，为单独的组员实现了组管理。
 * 通过拓展这个类可以实现 组语义(Group semantics )？ {@link ConsumerCoordinator} 是一个示例。
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * 从高层次来讲，Kafka的组管理协议由以下顺序步骤组成：
 *
 * - Group 注册：Group成员注册到Coordinator上，并提供自己metadata（比如说他们感兴趣的topics set）
 *
 * - leader选举：coordinator选择group的成员并且选择一个作为leader
 *
 * - state 分配：leader从所有group内成员中收集metadata并分配state
 *
 * - Group 稳定：每个member收到leader分配的state并开始处理
 *
 * <ol>
 * <li>Group Registration: Group members register with the coordinator providing their own metadata
 * (such as the set of topics they are interested in).</li>
 * <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 * as the leader.</li>
 * <li>State Assignment: The leader collects the metadata from all the members of the group and
 * assigns state.</li>
 * <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 * processing.</li>
 * </ol>
 *
 * 为了使用此协议需要实现
 * 在metadata()中 ，定义metadata每个成员在Group注册时提供的格式；
 * 在performAssignment() 中，定义leader 提供的 state 分配的格式；
 * 在onJoinComplete()中，定义如何成为可用的成员；todo 翻译可能不准确
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, Map)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 */
public abstract class AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);

    // 心跳任务的辅助类，其中记录了两次发送心跳消息的间隔，最近发送心跳的时间，最后受到心跳响应的时间
    // 过期时间，心跳任务充值时间，同时还提供了计算下次发送心跳的时间，检测是否过期的方法
    private final Heartbeat heartbeat;

    /** 是一个定时任务，负责定时发送心跳请求和心跳相应，会被添加到 {@link ConsumerNetworkClient#delayedTasks} */
    private final HeartbeatTask heartbeatTask;

    private final int sessionTimeoutMs;

    private final GroupCoordinatorMetrics sensors;

    /** 当前消费者所属的消费族id */
    protected final String groupId;

    /** ConsumerNetworkClient对象，负责网络通信和执行定时任务 */
    protected final ConsumerNetworkClient client;

    protected final Time time;

    protected final long retryBackoffMs;

    /** 标记是否需要执行发送JoinGroupRequest */
    private boolean needsJoinPrepare = true;

    /** 此字段是否需要重新发送JoinGroupRequest请求的条件之一 */
    private boolean rejoinNeeded = true;

    /** 记录服务端GroupCoordinator所在的Node节点 */
    protected Node coordinator;

    /** 服务端GroupCoordinator返回的分配给消费者的唯一Id */
    protected String memberId;

    protected String protocol;

    /**
     * GroupCoordinator返回的年代信息，用来区分两次Rebalance操作，由于网络延迟等问题，在执行Rebalance操作时
     * 可能收到上次Rebalance过程的请求，为了避免这种干扰，每次Rebalance操作都会递增generation的值
     */
    protected int generation;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
        String groupId,
        int sessionTimeoutMs,
        int heartbeatIntervalMs,
        Metrics metrics,
        String metricGrpPrefix,
        Time time,
        long retryBackoffMs) {
        this.client = client;
        this.time = time;
        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        this.groupId = groupId;
        this.coordinator = null;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, heartbeatIntervalMs, time.milliseconds());
        this.heartbeatTask = new HeartbeatTask();
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of protocols implements (e.g. "consumer" or "connect").
     *
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     *
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract List<ProtocolMetadata> metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     *
     * 优先调用每个组的join或rejoin。这代表性地用来从之前的世代进行任何cleanup操作（比如为消费者提交offset）。
     *
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     *
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     *
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
        String protocol,
        Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group.
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
        String memberId,
        String protocol,
        ByteBuffer memberAssignment);

    /**
     * Block until the coordinator for this group is known and is ready to receive requests.
     * 直到协调器为这个组所知并且准备好接收请求都会阻塞
     */
    public void ensureCoordinatorReady() {
        while (coordinatorUnknown()) {// 是否需要重新查找GroupCoordinator，主要检查coordinator字段是否为空，以及与GroupCoordinator
            // 之间的连接是否正常
            RequestFuture<Void> future = sendGroupCoordinatorRequest();// 创建并缓存请求
            client.poll(future);// 阻塞地发送请求

            if (future.failed()) {
                if (future.isRetriable()) {
                    client.awaitMetadataUpdate();
                } else {
                    throw future.exception();
                }
            } else if (coordinator != null && client.connectionFailed(coordinator)) {
                // 如果Coordinator 依旧为null，并且client连接Coordinator节点失败，这里sleep一段时间，然后继续查找Coordinator
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }
        }
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     *
     * 检查组是否需要rejoined，比如metadata变了
     *
     * @return true if it should, false otherwise
     */
    protected boolean needRejoin() {
        return rejoinNeeded;
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     * 确保group是有效的
     */
    public void ensureActiveGroup() {
        if (!needRejoin()) { // 不需要重新加入组
            return;
        }

        if (needsJoinPrepare) {
            onJoinPrepare(generation, memberId);// 这个准备会做三件事
            // 1、如果开启了自动提交offset则会进行同步提交offset（可能阻塞）
            // 2、调用 Subscription 中ConsumerRebalance中ConsumerRebalance中的回调方法
            // 3、设置needsPartitionAssignment 为true，收缩groupSubscription集合
            needsJoinPrepare = false;
        }

        while (needRejoin()) {
            ensureCoordinatorReady();// rebalance操作的第一步，查找GroupCoordinator，这个阶段会在kafka集群中的任意一个Broker发送GroupCoordinatorRequest请求，并处理返回的GroupCoordinatorResponse

            // ensure that there are no pending requests to the coordinator. This is important
            // in particular to avoid resending a pending JoinGroup request.
            if (client.pendingRequestCount(this.coordinator) > 0) {
                client.awaitPendingRequests(this.coordinator);
                continue;
            }

            // 1、在sendJoinGroupRequest()，首先发送一个 client 请求，将其compose成 RequestFuture<ByteBuffer> futureJoin
            // 在 client 成功后，会调用 onSuccess，触发 JoinGroupResponseHandler 的onSuccess
            //
            // 2、###### 在 JoinGroupResponseHandler 的处理中，会去chain这个 futureJoin
            //
            // 3、来到了SyncGroupResponseHandler，发送一个新的 client 请求，将其compose成 RequestFuture<ByteBuffer> futureSync，在这个client成功后，会调用 onSuccess，触发 SyncGroupResponseHandler 的onSuccess
            //
            // 4、在 SyncGroupResponseHandler的处理中，如果正确返回，触发之前chain这个的 futrueJoin，才是真正的成功了
            //
            // 5、成功后，调用了我们给 futrueJoin 添加的监听器。
            RequestFuture<ByteBuffer> future = sendJoinGroupRequest();
            future.addListener(new RequestFutureListener<ByteBuffer>() {

                @Override
                public void onSuccess(ByteBuffer value) {
                    // handle join completion in the callback so that the callback will be invoked
                    // even if the consumer is woken up before finishing the rebalance
                    onJoinComplete(generation, memberId, protocol, value);
                    needsJoinPrepare = true;
                    heartbeatTask.reset();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin
                }
            });

            client.poll(future);

            if (future.failed()) {
                RuntimeException exception = future.exception();
                if (exception instanceof UnknownMemberIdException ||
                    exception instanceof RebalanceInProgressException ||
                    exception instanceof IllegalGenerationException) {
                    continue;
                } else if (!future.isRetriable()) {
                    throw exception;
                }
                time.sleep(retryBackoffMs);
            }
        }
    }

    private class HeartbeatTask implements DelayedTask {

        private boolean requestInFlight = false;

        public void reset() {
            // start or restart the heartbeat task to be executed at the next chance
            long now = time.milliseconds();
            heartbeat.resetSessionTimeout(now);
            client.unschedule(this);

            if (!requestInFlight) {
                client.schedule(this, now);
            }
        }

        /**
         * 发送心跳包
         *
         * @param now current time in milliseconds
         */
        @Override
        public void run(final long now) {
            // 还没有 generation 或者 需要重新入组 或者 有没有连上 Coordinator
            if (generation < 0 || needRejoin() || coordinatorUnknown()) {
                // no need to send the heartbeat we're not using auto-assignment or if we are
                // awaiting a rebalance
                // 不需要发送心跳包因为我们没有使用 auto-assignment 自动分配 或者可能正等待Rebalance
                return;
            }

            // 是否超时
            if (heartbeat.sessionTimeoutExpired(now)) {
                // we haven't received a successful heartbeat in one session interval
                // so mark the coordinator dead
                // 没有在一个session间隔中收到一个成功的心跳包，所以标记Coordinator挂了
                coordinatorDead();
                return;
            }

            // 是否需要发送心跳包（有可能因为reset了的原因，导致需要另择时机重新发送）
            if (!heartbeat.shouldHeartbeat(now)) {// 不需发送
                // we don't need to heartbeat now, so reschedule for when we do
                // 现在不需要发送心跳包，所以重新schedule一下
                client.schedule(this, now + heartbeat.timeToNextHeartbeat(now));
            } else {// 需发送
                heartbeat.sentHeartbeat(now);
                // 请求在路上
                requestInFlight = true;

                // 发送心跳请求
                RequestFuture<Void> future = sendHeartbeatRequest();

                // 为适配后的 RequestFuture<心跳> 添加监听
                future.addListener(new RequestFutureListener<Void>() {

                    @Override
                    /**
                     * 这个succeeded 并不是在请求成功后调用，而是在
                     * 请求成功后，然后 {@link HeartbeatCompletionHandler#handle)}
                     * 处理error码，确认无问题后才调用
                     */
                    public void onSuccess(Void value) {
                        requestInFlight = false;// 代表已经拿到了返回包
                        long now = time.milliseconds();
                        heartbeat.receiveHeartbeat(now);// 标记最后一次接受心跳是现在

                        // 下次心跳包 = 现在 +
                        long nextHeartbeatTime = now + heartbeat.timeToNextHeartbeat(now);
                        client.schedule(HeartbeatTask.this, nextHeartbeatTime);
                    }

                    /**
                     * 同理
                     * @param e
                     */
                    @Override
                    public void onFailure(RuntimeException e) {
                        requestInFlight = false;
                        client.schedule(HeartbeatTask.this, time.milliseconds() + retryBackoffMs);
                    }
                });
            }
        }
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, Map)} if
     * elected leader by the coordinator.
     *
     * @return A request future which wraps the assignment returned from the group leader
     */
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown()) {
            return RequestFuture.coordinatorNotAvailable();
        }

        // send a join group request to the coordinator
        log.info("(Re-)joining group {}", groupId);
        JoinGroupRequest request = new JoinGroupRequest(
            groupId,
            this.sessionTimeoutMs,
            this.memberId,
            protocolType(),
            metadata());

        log.debug("Sending JoinGroup ({}) to coordinator {}", request, this.coordinator);
        return client.send(coordinator, ApiKeys.JOIN_GROUP, request)
                     .compose(new JoinGroupResponseHandler());
    }

    private class JoinGroupResponseHandler extends RequestFutureAdapter<ClientResponse, ByteBuffer> {

        private ClientResponse response;

        public void onFailure(RuntimeException e, RequestFuture<ByteBuffer> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                coordinatorDead();
            }
            future.raise(e);
        }

        public void onSuccess(ClientResponse clientResponse, RequestFuture<ByteBuffer> future) {
            try {
                this.response = clientResponse;
                JoinGroupResponse responseObj = parse(clientResponse);
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone()) {
                    future.raise(e);
                }
            }
        }

        public JoinGroupResponse parse(ClientResponse response) {
            return new JoinGroupResponse(response.responseBody());
        }

        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future/* sendJoinGroupRequest#joinGroupFuture */) {
            Errors error = Errors.forCode(joinResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("Received successful join group response for group {}: {}", groupId, joinResponse.toStruct());
                AbstractCoordinator.this.memberId = joinResponse.memberId();
                AbstractCoordinator.this.generation = joinResponse.generationId();

                // 正常收到JoinGroupResponse的相应，将rejoin设置为false
                AbstractCoordinator.this.rejoinNeeded = false;
                AbstractCoordinator.this.protocol = joinResponse.groupProtocol();
                sensors.joinLatency.record(response.requestLatencyMs());
                if (joinResponse.isLeader()) {// 判断是否为leader节点，比较leader节点与自己
                    // 这个传进去的future是sendJoinGroupRequest后的joinGroupFuture
                    // 当onJoinLeader方法完成后，再通知这个future
                    onJoinLeader(joinResponse).chain(future);
                } else {
                    onJoinFollower().chain(future);
                }
            } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId,
                    coordinator);
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // reset the member id and retry immediately
                AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                || error == Errors.INVALID_SESSION_TIMEOUT
                || error == Errors.INVALID_GROUP_ID) {
                // log the error and re-throw the exception
                log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest request = new SyncGroupRequest(groupId, generation,
            memberId, Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
        return sendSyncGroupRequest(request);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group

            // joinResponse.members : Map<String/* memberId */, ByteBuffer/* 包含它关注了那些 topic */>
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                joinResponse.members());

            SyncGroupRequest request = new SyncGroupRequest(groupId, generation, memberId, groupAssignment);
            log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
            return sendSyncGroupRequest(request);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest request) {
        if (coordinatorUnknown()) {
            return RequestFuture.coordinatorNotAvailable();
        }
        return client.send(coordinator, ApiKeys.SYNC_GROUP, request)
                     .compose(new SyncGroupResponseHandler());
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {

        @Override
        public SyncGroupResponse parse(ClientResponse response) {
            return new SyncGroupResponse(response.responseBody());
        }

        @Override
        public void handle(SyncGroupResponse syncResponse,
            RequestFuture<ByteBuffer> future) {
            Errors error = Errors.forCode(syncResponse.errorCode());
            if (error == Errors.NONE) {
                log.info("Successfully joined group {} with generation {}", groupId, generation);
                sensors.syncLatency.record(response.requestLatencyMs());
                future.complete(syncResponse.memberAssignment());
            } else {
                // 表示需要重新join
                AbstractCoordinator.this.rejoinNeeded = true;
                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.debug("SyncGroup for group {} failed due to coordinator rebalance", groupId);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                    || error == Errors.ILLEGAL_GENERATION) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                    future.raise(error);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    coordinatorDead();
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     *
     * 为group查找coordinator，发送一个GroupMetadata到其中一个broker，返回的future应该
     * 被polled来获取request的结果
     *
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupCoordinatorRequest() {
        // initiate the group metadata request
        // find a node to ask about the coordinator
        // 初始化 group metadata 请求
        // 找出一个节点来与Coordinator通信
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            // TODO: If there are no brokers left, perhaps we should use the bootstrap set
            // from configuration?
            return RequestFuture.noBrokersAvailable();
        } else {
            // create a group  metadata request
            log.debug("Sending coordinator request for group {} to broker {}", groupId, node);
            GroupCoordinatorRequest metadataRequest = new GroupCoordinatorRequest(this.groupId);

            // 实际上就是将请求先缓冲到consumerNetworkClient，
            return client.send(node, ApiKeys.GROUP_COORDINATOR, metadataRequest)
                         .compose(new RequestFutureAdapter<ClientResponse, Void>() {

                             @Override
                             public void onSuccess(ClientResponse response, RequestFuture<Void> future) {
                                 handleGroupMetadataResponse(response, future);
                             }
                         });
        }
    }

    private void handleGroupMetadataResponse(ClientResponse resp, RequestFuture<Void> future) {
        log.debug("Received group coordinator response {}", resp);

        if (!coordinatorUnknown()) {
            // We already found the coordinator, so ignore the request
            // 已经找到协调器了，所以不需要呼应这个request
            future.complete(null);
        } else {
            GroupCoordinatorResponse groupCoordinatorResponse = new GroupCoordinatorResponse(resp.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935

            // 使用 MAX_VALUE - node.id 作为Coordinator id ，在底层网络客户端层来为Coordinator模拟独立的连接
            Errors error = Errors.forCode(groupCoordinatorResponse.errorCode());
            if (error == Errors.NONE) {
                this.coordinator = new Node(Integer.MAX_VALUE - groupCoordinatorResponse.node()
                                                                                        .id(),
                    groupCoordinatorResponse.node()
                                            .host(),
                    groupCoordinatorResponse.node()
                                            .port());

                log.info("Discovered coordinator {} for group {}.", coordinator, groupId);

                client.tryConnect(coordinator);

                // start sending heartbeats only if we have a valid generation
                if (generation > 0) {
                    heartbeatTask.reset();// 启动心跳任务.. todo 还没看
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(error);
            }
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     *
     * 检查是否知道Coordinator是谁，并且我们有有效的连接。
     *
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        if (coordinator == null) {
            return true;
        }

        if (client.connectionFailed(coordinator)) {
            coordinatorDead();
            return true;
        }

        return false;
    }

    /**
     * Mark the current coordinator as dead.
     * 将Coordinator标记为死亡，实际上就是将ConsumerNetworkClient中相应的nodeId标记为失败
     * 并将coordinator字段设置为null
     */
    protected void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead for group {}", this.coordinator, groupId);
            client.failUnsentRequests(this.coordinator, GroupCoordinatorNotAvailableException.INSTANCE);
            this.coordinator = null;
        }
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public void close() {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        maybeLeaveGroup();
    }

    /**
     * Leave the current group and reset local generation/memberId.
     */
    public void maybeLeaveGroup() {
        client.unschedule(heartbeatTask);
        if (!coordinatorUnknown() && generation > 0) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            sendLeaveGroupRequest();
        }

        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        // 表示需要重新join
        rejoinNeeded = true;
    }

    private void sendLeaveGroupRequest() {
        LeaveGroupRequest request = new LeaveGroupRequest(groupId, memberId);
        RequestFuture<Void> future = client.send(coordinator, ApiKeys.LEAVE_GROUP, request)
                                           .compose(new LeaveGroupResponseHandler());

        future.addListener(new RequestFutureListener<Void>() {

            @Override
            public void onSuccess(Void value) {
            }

            @Override
            public void onFailure(RuntimeException e) {
                log.debug("LeaveGroup request for group {} failed with error", groupId, e);
            }
        });

        client.poll(future, 0);
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {

        @Override
        public LeaveGroupResponse parse(ClientResponse response) {
            return new LeaveGroupResponse(response.responseBody());
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            // process the response
            short errorCode = leaveResponse.errorCode();
            if (errorCode == Errors.NONE.code()) {
                future.complete(null);
            } else {
                future.raise(Errors.forCode(errorCode));
            }
        }
    }

    /**
     * Send a heartbeat request now (visible only for testing).
     */
    public RequestFuture<Void> sendHeartbeatRequest() {
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.memberId);

        return client.send(coordinator, ApiKeys.HEARTBEAT, req)
                     .compose(new HeartbeatCompletionHandler());
    }

    /**
     * 这个适配器并没有做什么特殊的逻辑处理，只是判断请求成功或者失败（失败的各种类型）来进行各种补偿操作，
     * 比如重新加入消费组之类的。
     */
    private class HeartbeatCompletionHandler extends CoordinatorResponseHandler<HeartbeatResponse/* 从这个 */, Void/* 到这个 */> {

        // 原 client.send(coordinator, ApiKeys.HEARTBEAT, req) 会返回的对象。
        protected ClientResponse response;

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<Void> future) {
            // 让我们看看在成功时都做了什么
            try {
                this.response = clientResponse;
                // 将clientResponse解析为心跳包 HeartbeatResponse
                HeartbeatResponse responseObj = parse(clientResponse);

                // 处理心跳包
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone()) {
                    future.raise(e);
                }
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                coordinatorDead();
            }
            future.raise(e);
        }

        @Override
        public HeartbeatResponse parse(ClientResponse response) {
            // 提取返回的body （Struct对象），new一个HeartbeatResponse
            return new HeartbeatResponse(response.responseBody());
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());

            // 将heartbeatResponse中short类型的 errCode 转为 Error对象
            Errors error = Errors.forCode(heartbeatResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("Received successful heartbeat response for group {}", groupId);

                // 没报错，直接将这个引用置为成功，成功后会调用 RequestFuture<Void> future 的 onSuccess方法
                future.complete(null);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE// 组协调器不可用
                || error == Errors.NOT_COORDINATOR_FOR_GROUP) {// 没有协调器
                log.debug("Attempt to heart beat failed for group {} since coordinator {} is either not started or not valid.",
                    groupId, coordinator);
                coordinatorDead();// 标记协调器挂了
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {// 当前组在重负载，所以需要重加入
                log.debug("Attempt to heart beat failed for group {} since it is rebalancing.", groupId);
                // 表示需要重新join
                AbstractCoordinator.this.rejoinNeeded = true;// 表示当前协调器需要重加入
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION) {// 世代错误
                log.debug("Attempt to heart beat failed for group {} since generation id is not legal.", groupId);
                // 表示需要重新join
                AbstractCoordinator.this.rejoinNeeded = true;// 表示当前协调器需要重加入
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {// 协调器不认识这个成员
                log.debug("Attempt to heart beat failed for group {} since member id is not valid.", groupId);
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                // 表示需要重新join
                AbstractCoordinator.this.rejoinNeeded = true;// 表示当前协调器需要重加入
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {// 组认证失败
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {

        protected ClientResponse response;

        public abstract R parse(ClientResponse response);

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                coordinatorDead();
            }
            future.raise(e);
        }

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = parse(clientResponse);
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone()) {
                    future.raise(e);
                }
            }
        }
    }

    private class GroupCoordinatorMetrics {

        public final Metrics metrics;

        public final String metricGrpName;

        public final Sensor heartbeatLatency;

        public final Sensor joinLatency;

        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(metrics.metricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second"), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                this.metricGrpName,
                "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                this.metricGrpName,
                "The max time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-rate",
                this.metricGrpName,
                "The number of group joins per second"), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                this.metricGrpName,
                "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                this.metricGrpName,
                "The max time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-rate",
                this.metricGrpName,
                "The number of group syncs per second"), new Rate(new Count()));

            Measurable lastHeartbeat =
                new Measurable() {

                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat"),
                lastHeartbeat);
        }
    }
}
