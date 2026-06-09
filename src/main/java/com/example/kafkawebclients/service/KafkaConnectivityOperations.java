package com.example.kafkawebclients.service;

import com.example.kafkawebclients.model.ConfigValidationResult;
import com.example.kafkawebclients.model.StreamConfig;
import reactor.core.publisher.Mono;

public interface KafkaConnectivityOperations {

    Mono<ConfigValidationResult> validate(StreamConfig config);
}
