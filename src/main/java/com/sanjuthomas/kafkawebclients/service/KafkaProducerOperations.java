package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.ProduceRequest;
import com.sanjuthomas.kafkawebclients.model.ProduceResult;
import reactor.core.publisher.Mono;

public interface KafkaProducerOperations {

    Mono<ProduceResult> produce(ProduceRequest request);
}
