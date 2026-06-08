package com.example.kafkabrowser.handler;

import com.example.kafkabrowser.model.StreamConfig;
import com.example.kafkabrowser.model.WebSocketMessage;
import com.example.kafkabrowser.service.KafkaStreamService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicReference;

@Component
public class KafkaStreamWebSocketHandler implements WebSocketHandler {

    private final KafkaStreamService kafkaStreamService;
    private final ObjectMapper objectMapper;

    public KafkaStreamWebSocketHandler(KafkaStreamService kafkaStreamService, ObjectMapper objectMapper) {
        this.kafkaStreamService = kafkaStreamService;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        Sinks.Many<WebSocketMessage> outboundSink = Sinks.many().unicast().onBackpressureBuffer();
        AtomicReference<Disposable> kafkaSubscription = new AtomicReference<>();

        Flux<org.springframework.web.reactive.socket.WebSocketMessage> outbound = outboundSink.asFlux()
                .map(this::toJson)
                .map(session::textMessage);

        Mono<Void> inbound = session.receive()
                .map(org.springframework.web.reactive.socket.WebSocketMessage::getPayloadAsText)
                .flatMap(payload -> handleClientMessage(payload, outboundSink, kafkaSubscription))
                .then();

        return session.send(outbound)
                .and(inbound)
                .doFinally(signal -> disposeKafka(kafkaSubscription));
    }

    private Mono<Void> handleClientMessage(
            String payload,
            Sinks.Many<WebSocketMessage> outboundSink,
            AtomicReference<Disposable> kafkaSubscription
    ) {
        WebSocketMessage message;
        try {
            message = objectMapper.readValue(payload, WebSocketMessage.class);
        } catch (JsonProcessingException e) {
            outboundSink.tryEmitNext(WebSocketMessage.error("Invalid message format"));
            return Mono.empty();
        }

        if (message.action() == null) {
            outboundSink.tryEmitNext(WebSocketMessage.error("Missing action"));
            return Mono.empty();
        }

        return switch (message.action()) {
            case "start" -> startStreaming(message.config(), outboundSink, kafkaSubscription);
            case "stop" -> stopStreaming(outboundSink, kafkaSubscription);
            default -> {
                outboundSink.tryEmitNext(WebSocketMessage.error("Unknown action: " + message.action()));
                yield Mono.empty();
            }
        };
    }

    private Mono<Void> startStreaming(
            StreamConfig config,
            Sinks.Many<WebSocketMessage> outboundSink,
            AtomicReference<Disposable> kafkaSubscription
    ) {
        if (config == null) {
            outboundSink.tryEmitNext(WebSocketMessage.error("Config is required to start streaming"));
            return Mono.empty();
        }

        if (config.bootstrapServers() == null || config.bootstrapServers().isBlank()) {
            outboundSink.tryEmitNext(WebSocketMessage.error("Bootstrap servers are required"));
            return Mono.empty();
        }

        if (config.topic() == null || config.topic().isBlank()) {
            outboundSink.tryEmitNext(WebSocketMessage.error("Topic name is required"));
            return Mono.empty();
        }

        disposeKafka(kafkaSubscription);

        Flux<WebSocketMessage> kafkaFlux = kafkaStreamService.startStreaming(config, outboundSink);
        Disposable disposable = kafkaStreamService.subscribe(kafkaFlux, outboundSink);
        kafkaSubscription.set(disposable);

        outboundSink.tryEmitNext(WebSocketMessage.status("Streaming started"));
        return Mono.empty();
    }

    private Mono<Void> stopStreaming(
            Sinks.Many<WebSocketMessage> outboundSink,
            AtomicReference<Disposable> kafkaSubscription
    ) {
        disposeKafka(kafkaSubscription);
        outboundSink.tryEmitNext(WebSocketMessage.status("Streaming stopped"));
        return Mono.empty();
    }

    private void disposeKafka(AtomicReference<Disposable> kafkaSubscription) {
        Disposable disposable = kafkaSubscription.getAndSet(null);
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
    }

    private String toJson(WebSocketMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            return "{\"type\":\"error\",\"error\":\"Failed to serialize message\"}";
        }
    }
}
