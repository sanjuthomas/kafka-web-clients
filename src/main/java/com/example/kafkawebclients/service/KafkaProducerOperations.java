package com.example.kafkawebclients.service;

import com.example.kafkawebclients.model.ProduceRequest;
import com.example.kafkawebclients.model.ProduceResult;
import reactor.core.publisher.Mono;

public interface KafkaProducerOperations {

    Mono<ProduceResult> produce(ProduceRequest request);
}
