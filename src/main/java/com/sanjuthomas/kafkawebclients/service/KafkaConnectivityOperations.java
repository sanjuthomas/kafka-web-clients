package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.ClusterConnectionRequest;
import com.sanjuthomas.kafkawebclients.model.ConfigValidationResult;
import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import reactor.core.publisher.Mono;

public interface KafkaConnectivityOperations {

    Mono<ConfigValidationResult> validate(StreamConfig config);

    Mono<ConfigValidationResult> validateCluster(ClusterConnectionRequest request);
}
