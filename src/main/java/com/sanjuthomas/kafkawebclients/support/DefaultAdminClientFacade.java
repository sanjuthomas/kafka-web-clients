package com.sanjuthomas.kafkawebclients.support;

import com.sanjuthomas.kafkawebclients.model.ConsumerGroupOffsetInfo;
import com.sanjuthomas.kafkawebclients.model.PartitionOffsetInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DefaultAdminClientFacade implements AdminClientFacade {

    private static final int CONNECT_TIMEOUT_SECONDS = 10;

    private final Admin admin;

    public DefaultAdminClientFacade(Admin admin) {
        this.admin = admin;
    }

    @Override
    public String clusterId() throws Exception {
        return admin.describeCluster()
                .clusterId()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public int partitionCount(String topic) throws Exception {
        var topicDescription = admin.describeTopics(Collections.singletonList(topic))
                .topicNameValues()
                .get(topic)
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return topicDescription.partitions().size();
    }

    @Override
    public List<String> listUserTopics() throws Exception {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(false);
        return admin.listTopics(options)
                .names()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .stream()
                .sorted()
                .toList();
    }

    @Override
    public void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
        NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
        admin.createTopics(Collections.singleton(newTopic))
                .all()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void deleteTopic(String topic) throws Exception {
        admin.deleteTopics(Collections.singleton(topic))
                .all()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public List<ConsumerGroupOffsetInfo> listConsumerGroupOffsetsForTopic(String topic) throws Exception {
        var groups = admin.listConsumerGroups()
                .all()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        List<ConsumerGroupOffsetInfo> result = new ArrayList<>();
        for (var group : groups) {
            String groupId = group.groupId();
            Map<TopicPartition, OffsetAndMetadata> offsets;
            try {
                offsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                continue;
            }

            List<PartitionOffsetInfo> topicOffsets = offsets.entrySet().stream()
                    .filter(entry -> entry.getKey().topic().equals(topic))
                    .map(entry -> new PartitionOffsetInfo(
                            entry.getKey().partition(),
                            entry.getValue().offset()))
                    .sorted(Comparator.comparingInt(PartitionOffsetInfo::partition))
                    .toList();

            if (!topicOffsets.isEmpty()) {
                result.add(new ConsumerGroupOffsetInfo(groupId, topicOffsets));
            }
        }

        result.sort(Comparator.comparing(ConsumerGroupOffsetInfo::groupId));
        return result;
    }

    @Override
    public void resetConsumerGroupOffset(String consumerGroup, String topic, long offset) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = admin.listConsumerGroupOffsets(consumerGroup)
                .partitionsToOffsetAndMetadata()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
            if (entry.getKey().topic().equals(topic)) {
                newOffsets.put(entry.getKey(), new OffsetAndMetadata(offset));
            }
        }

        if (newOffsets.isEmpty()) {
            int partitions = partitionCount(topic);
            for (int partition = 0; partition < partitions; partition++) {
                newOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
            }
        }

        admin.alterConsumerGroupOffsets(consumerGroup, newOffsets)
                .all()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        admin.close();
    }
}
