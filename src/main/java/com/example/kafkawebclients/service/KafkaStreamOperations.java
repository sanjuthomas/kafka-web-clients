package com.example.kafkawebclients.service;

import com.example.kafkawebclients.model.StreamConfig;
import com.example.kafkawebclients.model.WebSocketMessage;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public interface KafkaStreamOperations {

    Flux<WebSocketMessage> startStreaming(StreamConfig config, Sinks.Many<WebSocketMessage> controlSink);

    Disposable subscribe(Flux<WebSocketMessage> stream, Sinks.Many<WebSocketMessage> outputSink);
}
