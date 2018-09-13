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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Collection)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * 这是一个为consumer追踪topics、partitions、offsets的类。一个分区可由{@link #assignFromUser(Collection)}直接“分配”（手动分配）
 * 或者使用{@link #assignFromSubscribed(Collection)} 自动分配
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * 一次分配，一个partition将不被视为“fetchable”直到它的初始position被{@link #seek(TopicPartition, long)}设置。
 * Fetchable的 partitions 追踪一个 fetch position， 用来设置下次fetch时【传入的offset】，
 * 被消费的position就是最后一个返回给user的offset。你可以通过{@link #pause(TopicPartition)} 延迟从一个partition获取，
 * 并且它不会影响到 fetched/consumed的offsets。partition 将会保留 unfetchable 直到使用{@link #resume(TopicPartition)}
 * 你也可以使用 {@link #isPaused(TopicPartition)}单独查询暂停状态
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, OffsetAndMetadata)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher#resetOffset(TopicPartition)}.
 */
public class SubscriptionState {

    /** 订阅tipic的模式 */
    private enum SubscriptionType {
        NONE,
        /** 按照指定的topic名字进行订阅，自动匹配partition */
        AUTO_TOPICS,
        /** 按照指定的正则去匹配Topic进行订阅，自动匹配partition */
        AUTO_PATTERN,
        /** 用户手动指定消费者消费的topic及其partition */
        USER_ASSIGNED
    }

    /* the type of subscription */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */

    /** 使用AUTO_PATTERN模式时，记录正则 */
    private Pattern subscribedPattern;

    /** 如果是AUTO_TOPICS或者AUTO_PATTERN 则此集合记录所有的topic，仅有{@link #changeSubscription(Collection)}可以向其添加数据 */
    /* the list of topics the user has requested */
    private final Set<String> subscription;

    /** 如果使用USER_ASSIGNED模式，则次记录记录了分配给当前消费者的TopicPartition集合，这个集合与{@link #subscription}是互斥的 */
    /* the list of partitions the user has requested */
    private final Set<TopicPartition> userAssignment;

    /** 记录每个TopicPartition的消费状态 */
    /* the list of partitions currently assigned */
    private final Map<TopicPartition, TopicPartitionState> assignment;

    /** consumer group会选举一个leader，leader使用这个集合组中所有消费者订阅的topic，follower只保存自己订阅的topic */
    /* the list of topics the group has subscribed to (set only for the leader on join group completion) */
    private final Set<String> groupSubscription;

    /* do we need to request a partition assignment from the coordinator? */

    /** 是否需要由coordinator进行一次分区分配 */
    private boolean needsPartitionAssignment;

    /**
     * do we need to request the latest committed offsets from the coordinator?
     * 标记是否需要从GroupCoordinator获取最近提交的offset，当出现异步提交offset操作或者是ReBalance操作
     * 完成时会将其设置为true，成功获取最近提交offset之后会设置为false
     */
    private boolean needsFetchCommittedOffsets;

    /* Default offset reset strategy */

    /** 默认的offset重置策略 */
    private final OffsetResetStrategy defaultResetStrategy;

    /* Listener to be invoked when assignment changes */
    private ConsumerRebalanceListener listener;

    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
        "Subscription to topics, partitions and pattern are mutually exclusive";

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     *
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE) {
            this.subscriptionType = type;
        } else if (this.subscriptionType != type) {
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        }
    }

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.userAssignment = new HashSet<>();
        this.assignment = new HashMap<>();
        this.groupSubscription = new HashSet<>();
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /** 这里使用的是AUTO_TOPICS 订阅模式， */
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        }

        setSubscriptionType(SubscriptionType.AUTO_TOPICS);

        this.listener = listener;

        changeSubscription(topics);
    }

    /**
     * 将消费者自身订阅的Topic添加到{@link #groupSubscription}集合，重置{@link #subscription}集合
     *
     * 这里将needsPartitionAssignment设置为true是因为消费者topic发生了变化，所以需要进行分区分配
     */
    public void changeSubscription(Collection<String> topicsToSubscribe) {
        // 如果订阅的topic有变化
        if (!this.subscription.equals(new HashSet<>(topicsToSubscribe))) {
            this.subscription.clear();
            this.subscription.addAll(topicsToSubscribe);
            this.groupSubscription.addAll(topicsToSubscribe);
            this.needsPartitionAssignment = true;

            // Remove any assigned partitions which are no longer subscribed to
            // 移除不再订阅的任何不再分配的分区
            for (Iterator<TopicPartition> it = assignment.keySet()
                                                         .iterator(); it.hasNext(); ) {
                TopicPartition tp = it.next();
                if (!subscription.contains(tp.topic())) {
                    it.remove();
                }
            }
        }
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     *
     * 将topics添加到 {@link #groupSubscription}，group leader用它来确保自己接收了所有
     * 组内感兴趣的topics的元数据更新
     *
     * @param topics The topics to add to the group subscription
     */
    public void groupSubscribe(Collection<String> topics) {
        if (this.subscriptionType == SubscriptionType.USER_ASSIGNED) {
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        }
        this.groupSubscription.addAll(topics);
    }

    /**
     * 这里的场景比较复杂，调用这里将needsPartitionAssignment设置为true，
     * 主要是因为在某些请求响应中出现了 ILLEGAL_GENERATION等异常，或者
     * 订阅的Topic出现了分区数量的变化
     *
     * 将组中其他订阅的topic删除，只留自己的
     */
    public void needReassignment() {
        this.groupSubscription.retainAll(subscription);
        this.needsPartitionAssignment = true;
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     *
     * 将分配更改为用户提供的指定分区，注意这和{@link #assignFromSubscribed(Collection)}不同，
     * 它输入的分区时由订阅的topic来提供的
     *
     * 这里使用了用户分配模式，所以不需要needsPartitionAssignment
     */
    public void assignFromUser(Collection<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        this.userAssignment.clear();
        this.userAssignment.addAll(partitions);

        for (TopicPartition partition : partitions)
            if (!assignment.containsKey(partition)) {
                addAssignedPartition(partition);
            }

        this.assignment.keySet()
                       .retainAll(this.userAssignment);

        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true;
    }

    /**
     * 成功饿到SyncGroupResponse中的分区分配结果时进行的操作，此时ReBalance操作结束，所以将needsPartitionAssignment = false
     *
     * Change the assignment to the specified partitions returned from the coordinator,
     * note this is different from {@link #assignFromUser(Collection)} which directly set the assignment from user inputs
     *
     * 将分配更改为coordinator协调器返回的指定分区，注意这个和 {@link #assignFromUser(Collection)}不同，
     * 它直接从用户的分配来设置分配。
     */
    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        for (TopicPartition tp : assignments)
            if (!this.subscription.contains(tp.topic())) {
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
            }
        this.assignment.clear();
        for (TopicPartition tp : assignments)
            addAssignedPartition(tp);
        this.needsPartitionAssignment = false;
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        }

        setSubscriptionType(SubscriptionType.AUTO_PATTERN);

        this.listener = listener;
        this.subscribedPattern = pattern;
    }

    public boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    /**
     * 不再进行订阅，也需要重新进行分区分配
     */
    public void unsubscribe() {
        this.subscription.clear();
        this.userAssignment.clear();
        this.assignment.clear();
        this.needsPartitionAssignment = true;
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    public Pattern getSubscribedPattern() {
        return this.subscribedPattern;
    }

    public Set<String> subscription() {
        return this.subscription;
    }

    public Set<TopicPartition> pausedPartitions() {
        HashSet<TopicPartition> paused = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final TopicPartitionState state = entry.getValue();
            if (state.paused) {
                paused.add(tp);
            }
        }
        return paused;
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     * of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    public Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.get(tp);
        if (state == null) {
            throw new IllegalStateException("No current assignment for partition " + tp);
        }
        return state;
    }

    public void committed(TopicPartition tp, OffsetAndMetadata offset) {
        assignedState(tp).committed(offset);
    }

    public OffsetAndMetadata committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.keySet();
    }

    public Set<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> fetchable = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            if (entry.getValue()
                     .isFetchable()) {
                fetchable.add(entry.getKey());
            }
        }
        return fetchable;
    }

    /**
     * SubscriptionType.AUTO_TOPICS 或 SubscriptionType.AUTO_PATTERN;
     */
    public boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public void position(TopicPartition tp, long offset) {
        assignedState(tp).position(offset);
    }

    public Long position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            TopicPartitionState state = entry.getValue();
            if (state.hasValidPosition()) {
                allConsumed.put(entry.getKey(), new OffsetAndMetadata(state.position));
            }
        }
        return allConsumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).awaitReset(offsetResetStrategy);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    public boolean hasAllFetchPositions() {
        for (TopicPartitionState state : assignment.values())
            if (!state.hasValidPosition()) {
                return false;
            }
        return true;
    }

    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet())
            if (!entry.getValue()
                      .hasValidPosition()) {
                missing.add(entry.getKey());
            }
        return missing;
    }

    public boolean partitionAssignmentNeeded() {
        return this.needsPartitionAssignment;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.containsKey(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    private void addAssignedPartition(TopicPartition tp) {
        this.assignment.put(tp, new TopicPartitionState());
    }

    public ConsumerRebalanceListener listener() {
        return listener;
    }

    /** 表示topicPartition的消费状态 */
    private static class TopicPartitionState {

        /** 下一次要从kafka获取的消息的offset */
        private Long position; // last consumed position

        /** 最近一次提交的offset */
        private OffsetAndMetadata committed;  // last committed position

        /** 当前partition是否处于暂停状态 */
        private boolean paused;  // whether this partition has been paused by the user

        /** 重置position的策略，如果为null，表示不进行重置 */
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting

        public TopicPartitionState() {
            this.paused = false;
            this.position = null;
            this.committed = null;
            this.resetStrategy = null;
        }

        private void awaitReset(OffsetResetStrategy strategy) {
            this.resetStrategy = strategy;
            this.position = null;
        }

        public boolean awaitingReset() {
            return resetStrategy != null;
        }

        public boolean hasValidPosition() {
            return position != null;
        }

        private void seek(long offset) {
            this.position = offset;
            this.resetStrategy = null;
        }

        private void position(long offset) {
            if (!hasValidPosition()) {
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            }
            this.position = offset;
        }

        private void committed(OffsetAndMetadata offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }
    }
}
