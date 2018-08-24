/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

/**
 * The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0: [t0p0, t0p1, t1p0, t1p1]
 * C1: [t0p2, t1p2]
 */
public class RangeAssignor extends AbstractPartitionAssignor {

    @Override
    public String name() {
        return "range";
    }

    /**
     * 将原本的 member订阅了哪几个topic，转换为topic 被哪几个member订阅了
     */
    private Map<String/* topic */, List<String/* memberId */>> consumersPerTopic(Map<String/* memberId */, List<String/* topic */>> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String/* memberId */, List<String/* topic */>> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue())
                // 这个put避免了重复了key，将重复的key合并在了同一个list中
                put(res, topic, consumerId);
        }
        return res;
    }

    /**
     * 针对每个Topic，n=分区数/消费者数量，m=分区数%消费者数量，前m个消费者每个分配n+1个分区，后面的
     * (消费者数量-m)个消费者每个分配n个partition
     *
     * @param partitionsPerTopic 每个订阅topic的分区数，如果元数据中不包含metadata将会从这个map中剔除
     * @param subscriptions memberId 与各自 topic subscription 的映射
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String/* topic */, Integer> partitionsPerTopic,
        Map<String/* memberId */, List<String/* topic */>> subscriptions) {

        // 将原本的 member订阅了哪几个topic，转换为topic 被哪几个member订阅了 topicPerConsumer => consumerPerTopic
        Map<String/* topic */, List<String/* memberId */>> consumersPerTopic = consumersPerTopic(subscriptions);

        // 为每个 topic new一个空的TopicPartition订阅集合
        Map<String/* memberId */, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());

        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<String/* memberId */> consumersForTopic = topicEntry.getValue();

            // 根据topic名字从 partitionsPerTopic 到partition下有多少个topic
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null) {
                continue;
            }

            Collections.sort(consumersForTopic);

            int numConsumersForConsumer = consumersForTopic.size();

            int N_numPartitionsPerConsumer = numPartitionsForTopic / numConsumersForConsumer;
            int M_consumersWithExtraPartition = numPartitionsForTopic % numConsumersForConsumer;

            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);

            // n = 分区数/消费者，m = 分区数%消费者，前m个消费者分配n+1个分区（每有一个余数，分配多一个），
            // 后面的消费者消费n个分区
            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                int start = N_numPartitionsPerConsumer * i + Math.min(i, M_consumersWithExtraPartition);
                int length = N_numPartitionsPerConsumer + (i + 1 > M_consumersWithExtraPartition ? 0 : 1);
                assignment.get(consumersForTopic.get(i))
                          .addAll(partitions.subList(start, start + length));
            }
        }
        return assignment;
    }
}
