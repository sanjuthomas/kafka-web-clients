package com.sanjuthomas.kafkawebclients.controller;

import com.sanjuthomas.kafkawebclients.model.AdminResult;
import com.sanjuthomas.kafkawebclients.model.ClusterConnectionRequest;
import com.sanjuthomas.kafkawebclients.model.ConsumerGroupOffsetsResult;
import com.sanjuthomas.kafkawebclients.model.CreateTopicRequest;
import com.sanjuthomas.kafkawebclients.model.ResetOffsetRequest;
import com.sanjuthomas.kafkawebclients.model.TopicAdminRequest;
import com.sanjuthomas.kafkawebclients.model.TopicListResult;
import com.sanjuthomas.kafkawebclients.service.KafkaAdminOperations;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final KafkaAdminOperations kafkaAdminOperations;

    public AdminController(KafkaAdminOperations kafkaAdminOperations) {
        this.kafkaAdminOperations = kafkaAdminOperations;
    }

    @PostMapping("/topics/list")
    public Mono<TopicListResult> listTopics(@RequestBody ClusterConnectionRequest request) {
        return kafkaAdminOperations.listTopics(request);
    }

    @PostMapping("/topics/create")
    public Mono<AdminResult> createTopic(@RequestBody CreateTopicRequest request) {
        return kafkaAdminOperations.createTopic(request);
    }

    @PostMapping("/topics/delete")
    public Mono<AdminResult> deleteTopic(@RequestBody TopicAdminRequest request) {
        return kafkaAdminOperations.deleteTopic(request);
    }

    @PostMapping("/consumer-groups")
    public Mono<ConsumerGroupOffsetsResult> listConsumerGroups(@RequestBody TopicAdminRequest request) {
        return kafkaAdminOperations.listConsumerGroups(request);
    }

    @PostMapping("/consumer-groups/reset-offset")
    public Mono<AdminResult> resetOffset(@RequestBody ResetOffsetRequest request) {
        return kafkaAdminOperations.resetOffset(request);
    }
}
