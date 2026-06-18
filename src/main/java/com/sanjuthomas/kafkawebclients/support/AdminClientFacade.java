package com.sanjuthomas.kafkawebclients.support;

import com.sanjuthomas.kafkawebclients.model.ConsumerGroupOffsetInfo;

import java.util.List;

public interface AdminClientFacade extends AutoCloseable {

    String clusterId() throws Exception;

    int partitionCount(String topic) throws Exception;

    List<String> listUserTopics() throws Exception;

    void createTopic(String topic, int partitions, short replicationFactor) throws Exception;

    void deleteTopic(String topic) throws Exception;

    List<ConsumerGroupOffsetInfo> listConsumerGroupOffsetsForTopic(String topic) throws Exception;

    void resetConsumerGroupOffset(String consumerGroup, String topic, long offset) throws Exception;

    @Override
    void close();
}
