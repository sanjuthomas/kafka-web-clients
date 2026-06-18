package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.AdminResult;
import com.sanjuthomas.kafkawebclients.model.ClusterConnectionRequest;
import com.sanjuthomas.kafkawebclients.model.ConsumerGroupOffsetsResult;
import com.sanjuthomas.kafkawebclients.model.CreateTopicRequest;
import com.sanjuthomas.kafkawebclients.model.ResetOffsetRequest;
import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import com.sanjuthomas.kafkawebclients.model.TopicAdminRequest;
import com.sanjuthomas.kafkawebclients.model.TopicListResult;
import com.sanjuthomas.kafkawebclients.support.AdminClientFacade;
import com.sanjuthomas.kafkawebclients.support.AdminClientFacadeFactory;
import com.sanjuthomas.kafkawebclients.support.KafkaConfigSupport;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;

@Service
public class KafkaAdminService implements KafkaAdminOperations {

    private final KafkaConfigSupport kafkaConfigSupport;
    private final AdminClientFacadeFactory adminClientFacadeFactory;

    public KafkaAdminService(
            KafkaConfigSupport kafkaConfigSupport,
            AdminClientFacadeFactory adminClientFacadeFactory
    ) {
        this.kafkaConfigSupport = kafkaConfigSupport;
        this.adminClientFacadeFactory = adminClientFacadeFactory;
    }

    @Override
    public Mono<TopicListResult> listTopics(ClusterConnectionRequest request) {
        String bootstrapError = validateBootstrapServers(request == null ? null : request.bootstrapServers());
        if (bootstrapError != null) {
            return Mono.just(TopicListResult.failure(bootstrapError));
        }

        return Mono.fromCallable(() -> {
            try (AdminClientFacade admin = createAdmin(request.bootstrapServers(), request.additionalProperties())) {
                return TopicListResult.success(admin.listUserTopics());
            }
        })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorReturn(TopicListResult.failure("Could not list topics. Please try again."));
    }

    @Override
    public Mono<AdminResult> createTopic(CreateTopicRequest request) {
        if (request == null) {
            return Mono.just(AdminResult.failure("Request is required"));
        }

        String bootstrapError = validateBootstrapServers(request.bootstrapServers());
        if (bootstrapError != null) {
            return Mono.just(AdminResult.failure(bootstrapError));
        }

        String topicError = validateTopic(request.topic());
        if (topicError != null) {
            return Mono.just(AdminResult.failure(topicError));
        }

        int partitions = request.partitions() != null ? request.partitions() : 1;
        short replicationFactor = request.replicationFactor() != null ? request.replicationFactor() : 1;
        if (partitions < 1) {
            return Mono.just(AdminResult.failure("Partitions must be at least 1"));
        }
        if (replicationFactor < 1) {
            return Mono.just(AdminResult.failure("Replication factor must be at least 1"));
        }

        String topic = request.topic().trim();
        return Mono.fromCallable(() -> {
            try (AdminClientFacade admin = createAdmin(request.bootstrapServers(), request.additionalProperties())) {
                admin.createTopic(topic, partitions, replicationFactor);
                return AdminResult.success("Created topic '" + topic + "' with "
                        + partitions + " partition(s) and replication factor " + replicationFactor + ".");
            }
        })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> Mono.just(AdminResult.failure(toUserFriendlyError(error, topic, null))));
    }

    @Override
    public Mono<AdminResult> deleteTopic(TopicAdminRequest request) {
        String validationError = validateTopicRequest(request);
        if (validationError != null) {
            return Mono.just(AdminResult.failure(validationError));
        }

        String topic = request.topic().trim();
        return Mono.fromCallable(() -> {
            try (AdminClientFacade admin = createAdmin(request.bootstrapServers(), request.additionalProperties())) {
                admin.deleteTopic(topic);
                return AdminResult.success("Deleted topic '" + topic + "'.");
            }
        })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> Mono.just(AdminResult.failure(toUserFriendlyError(error, topic, null))));
    }

    @Override
    public Mono<ConsumerGroupOffsetsResult> listConsumerGroups(TopicAdminRequest request) {
        String validationError = validateTopicRequest(request);
        if (validationError != null) {
            return Mono.just(ConsumerGroupOffsetsResult.failure(validationError));
        }

        String topic = request.topic().trim();
        return Mono.fromCallable(() -> {
            try (AdminClientFacade admin = createAdmin(request.bootstrapServers(), request.additionalProperties())) {
                return ConsumerGroupOffsetsResult.success(admin.listConsumerGroupOffsetsForTopic(topic));
            }
        })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> Mono.just(ConsumerGroupOffsetsResult.failure(
                        toUserFriendlyError(error, topic, null))));
    }

    @Override
    public Mono<AdminResult> resetOffset(ResetOffsetRequest request) {
        if (request == null) {
            return Mono.just(AdminResult.failure("Request is required"));
        }

        String bootstrapError = validateBootstrapServers(request.bootstrapServers());
        if (bootstrapError != null) {
            return Mono.just(AdminResult.failure(bootstrapError));
        }

        String topicError = validateTopic(request.topic());
        if (topicError != null) {
            return Mono.just(AdminResult.failure(topicError));
        }

        if (request.consumerGroup() == null || request.consumerGroup().isBlank()) {
            return Mono.just(AdminResult.failure("Consumer group is required"));
        }

        if (request.offset() == null) {
            return Mono.just(AdminResult.failure("Offset is required"));
        }

        if (request.offset() < 0) {
            return Mono.just(AdminResult.failure("Offset must be zero or greater"));
        }

        String topic = request.topic().trim();
        String consumerGroup = request.consumerGroup().trim();
        long offset = request.offset();

        return Mono.fromCallable(() -> {
            try (AdminClientFacade admin = createAdmin(request.bootstrapServers(), request.additionalProperties())) {
                admin.resetConsumerGroupOffset(consumerGroup, topic, offset);
                return AdminResult.success("Reset committed offsets for consumer group '"
                        + consumerGroup + "' on topic '" + topic + "' to " + offset + ".");
            }
        })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> Mono.just(AdminResult.failure(
                        toUserFriendlyError(error, topic, consumerGroup))));
    }

    private AdminClientFacade createAdmin(String bootstrapServers, String additionalProperties) {
        StreamConfig config = new StreamConfig(
                bootstrapServers.trim(),
                "admin",
                additionalProperties == null ? "" : additionalProperties,
                null,
                null);
        Map<String, Object> adminProps = kafkaConfigSupport.buildAdminProperties(config);
        return adminClientFacadeFactory.create(adminProps);
    }

    private String validateTopicRequest(TopicAdminRequest request) {
        if (request == null) {
            return "Request is required";
        }
        String bootstrapError = validateBootstrapServers(request.bootstrapServers());
        if (bootstrapError != null) {
            return bootstrapError;
        }
        return validateTopic(request.topic());
    }

    private String validateBootstrapServers(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            return "Bootstrap servers are required";
        }
        return null;
    }

    private String validateTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            return "Topic name is required";
        }
        return null;
    }

    private String toUserFriendlyError(Throwable error, String topic, String consumerGroup) {
        Throwable root = unwrap(error);

        if (root instanceof UnknownTopicOrPartitionException) {
            return "Topic '" + topic + "' was not found on the cluster.";
        }
        if (root instanceof TopicExistsException) {
            return "Topic '" + topic + "' already exists.";
        }
        if (root instanceof InvalidTopicException) {
            return "Invalid topic name: '" + topic + "'.";
        }
        if (root instanceof GroupIdNotFoundException) {
            return "Consumer group '" + consumerGroup + "' was not found.";
        }

        String message = root.getMessage();
        if (message != null && !message.isBlank()) {
            return message;
        }

        return "Kafka admin operation failed. Please try again.";
    }

    private Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
