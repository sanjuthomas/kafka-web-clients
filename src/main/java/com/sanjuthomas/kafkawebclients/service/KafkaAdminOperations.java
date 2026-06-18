package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.AdminResult;
import com.sanjuthomas.kafkawebclients.model.ClusterConnectionRequest;
import com.sanjuthomas.kafkawebclients.model.ConsumerGroupOffsetsResult;
import com.sanjuthomas.kafkawebclients.model.CreateTopicRequest;
import com.sanjuthomas.kafkawebclients.model.ResetOffsetRequest;
import com.sanjuthomas.kafkawebclients.model.TopicAdminRequest;
import com.sanjuthomas.kafkawebclients.model.TopicListResult;
import reactor.core.publisher.Mono;

public interface KafkaAdminOperations {

    Mono<TopicListResult> listTopics(ClusterConnectionRequest request);

    Mono<AdminResult> createTopic(CreateTopicRequest request);

    Mono<AdminResult> deleteTopic(TopicAdminRequest request);

    Mono<ConsumerGroupOffsetsResult> listConsumerGroups(TopicAdminRequest request);

    Mono<AdminResult> resetOffset(ResetOffsetRequest request);
}
