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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 *
 * Abstract assignor 实现了一些常见而繁杂的操作（经常要在assignors中做的 particular收集  分区数）
 */
public abstract class AbstractPartitionAssignor implements PartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    /**
     * Perform the group assignment given the partition counts and member subscriptions
     *
     * 执行组的分配后，将得到分区数和分区成员的详情
     *
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     * from this map. 每个订阅topic的分区数，如果元数据中不包含metadata将会从这个map中剔除
     * @param subscriptions Map from the memberId to their respective topic subscription
     * memberId 与各自 topic subscription 的映射
     *
     * @return Map from each member to the list of partitions assigned to them.
     */
    public abstract Map<String/* memberId */, List<TopicPartition>> assign(Map<String/* topic */, Integer> partitionsPerTopic,
        Map<String/* memberId */, List<String/* topic */>> subscriptions);

    @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics));
    }

    @Override
    public Map<String/* memberId */, Assignment> assign(Cluster metadata, Map<String/* memberId */, Subscription/* 包含它关注了那些 topic */> subscriptions) {
        // 所有订阅的Topic
        Set<String> allSubscribedTopics = new HashSet<>();

        // 目前来看topicSubscriptions就是把subscriptions里面的TopicList取出来了，然后把userData去掉了
        // 为什么怎么说，因为Subscription里面还有一个userData()
        Map<String/* memberId */, List<String/* topic */>> topicSubscriptions = new HashMap<>();

        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            List<String> topics = subscriptionEntry.getValue()
                                                   .topics();
            allSubscribedTopics.addAll(topics);
            topicSubscriptions.put(subscriptionEntry.getKey(), topics);
        }

        Map<String/* topic */, Integer/* partitionsNum */> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {// 循环所有订阅的Topic

            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0) {
                partitionsPerTopic.put(topic, numPartitions);
            } else {
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
            }
        }

        // 将分区分配的逻辑委托给了assign重载，子类可自由实现
        Map<String/* memberId */, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, topicSubscriptions);

        // this class has maintains no user data, so just wrap the results
        // 这个类不维护 user data，所以只是包装一下结果
        Map<String/* memberId */, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String/* memberId */, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return assignments;
    }

    @Override
    public void onAssignment(Assignment assignment) {
        // this assignor maintains no internal state, so nothing to do
    }

    /**
     * 这个put避免了重复了key，将重复的key合并在了同一个list中
     */
    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.add(value);
    }

    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
    }
}
