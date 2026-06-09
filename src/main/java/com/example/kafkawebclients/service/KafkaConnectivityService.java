package com.example.kafkawebclients.service;

import com.example.kafkawebclients.model.ConfigValidationResult;
import com.example.kafkawebclients.model.StreamConfig;
import com.example.kafkawebclients.support.KafkaConfigSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaConnectivityService {

    private static final int CONNECT_TIMEOUT_SECONDS = 10;

    private final KafkaConfigSupport kafkaConfigSupport;

    public KafkaConnectivityService(KafkaConfigSupport kafkaConfigSupport) {
        this.kafkaConfigSupport = kafkaConfigSupport;
    }

    public Mono<ConfigValidationResult> validate(StreamConfig config) {
        if (config == null) {
            return Mono.just(ConfigValidationResult.failure("Configuration is required"));
        }

        if (config.bootstrapServers() == null || config.bootstrapServers().isBlank()) {
            return Mono.just(ConfigValidationResult.failure("Bootstrap servers are required"));
        }

        if (config.topic() == null || config.topic().isBlank()) {
            return Mono.just(ConfigValidationResult.failure("Topic name is required"));
        }

        return Mono.fromCallable(() -> validateBlocking(config))
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorReturn(ConfigValidationResult.failure(
                        "Could not validate Kafka connectivity. Please try again."));
    }

    private ConfigValidationResult validateBlocking(StreamConfig config) {
        Map<String, Object> adminProps = kafkaConfigSupport.buildAdminProperties(config);
        String bootstrapServers = config.bootstrapServers().trim();
        String topic = config.topic().trim();

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.describeCluster()
                    .clusterId()
                    .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            var topicDescription = adminClient.describeTopics(Collections.singletonList(topic))
                    .topicNameValues()
                    .get(topic)
                    .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            int partitionCount = topicDescription.partitions().size();

            return ConfigValidationResult.success(
                    "Connected to Kafka. Topic '" + topic + "' exists with " + partitionCount + " partition(s).");
        } catch (ExecutionException e) {
            return ConfigValidationResult.failure(toUserFriendlyError(e.getCause() != null ? e.getCause() : e, config));
        } catch (java.util.concurrent.TimeoutException e) {
            return ConfigValidationResult.failure(
                    "Could not connect to Kafka at " + bootstrapServers
                            + ". The broker did not respond within " + CONNECT_TIMEOUT_SECONDS + " seconds.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ConfigValidationResult.failure("Kafka connectivity check was interrupted");
        } catch (Exception e) {
            return ConfigValidationResult.failure(toUserFriendlyError(e, config));
        }
    }

    private String toUserFriendlyError(Throwable error, StreamConfig config) {
        Throwable root = unwrap(error);
        String bootstrapServers = config.bootstrapServers().trim();
        String topic = config.topic().trim();

        if (root instanceof UnknownTopicOrPartitionException) {
            return "Topic '" + topic + "' was not found on the cluster at " + bootstrapServers + ".";
        }
        if (root instanceof TopicAuthorizationException) {
            return "Not authorized to access topic '" + topic + "'.";
        }
        if (root instanceof ClusterAuthorizationException) {
            return "Not authorized to connect to the Kafka cluster at " + bootstrapServers + ".";
        }
        if (root instanceof AuthenticationException) {
            return "Kafka authentication failed. Check your credentials in additional properties.";
        }
        if (root instanceof InvalidTopicException) {
            return "Invalid topic name: '" + topic + "'.";
        }
        if (root instanceof UnknownHostException) {
            return "Could not resolve a Kafka host. Check bootstrap servers: " + bootstrapServers + ".";
        }
        if (root instanceof ConnectException) {
            return "Could not connect to Kafka at " + bootstrapServers
                    + ". The broker may be down or unreachable.";
        }
        if (root instanceof TimeoutException || root instanceof java.util.concurrent.TimeoutException) {
            return "Could not connect to Kafka at " + bootstrapServers
                    + ". Check that the broker is running and reachable.";
        }

        String message = root.getMessage();
        if (message != null) {
            String lower = message.toLowerCase();
            if (lower.contains("timed out waiting for a node assignment")
                    || lower.contains("could not be established")
                    || lower.contains("node may not be available")
                    || lower.contains("connection refused")
                    || lower.contains("connection timed out")) {
                return "Could not connect to Kafka at " + bootstrapServers
                        + ". Check that the broker is running and reachable.";
            }
            if (lower.contains("unknown host") || lower.contains("nodename nor servname")) {
                return "Could not resolve a Kafka host. Check bootstrap servers: " + bootstrapServers + ".";
            }
            if (lower.contains("authentication") || lower.contains("sasl")) {
                return "Kafka authentication failed. Check your credentials in additional properties.";
            }
            if (lower.contains("ssl") || lower.contains("certificate")) {
                return "Kafka SSL/TLS connection failed. Check your security settings in additional properties.";
            }
            if (lower.contains("unknown topic") || lower.contains("unknown topic or partition")) {
                return "Topic '" + topic + "' was not found on the cluster at " + bootstrapServers + ".";
            }
        }

        return "Could not connect to Kafka at " + bootstrapServers
                + ". Check the broker address and additional properties.";
    }

    private Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
