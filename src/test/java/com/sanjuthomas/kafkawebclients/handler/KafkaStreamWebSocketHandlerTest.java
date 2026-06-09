package com.sanjuthomas.kafkawebclients.handler;

import com.sanjuthomas.kafkawebclients.service.KafkaStreamOperations;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaStreamWebSocketHandlerTest {

    private static final DefaultDataBufferFactory BUFFER_FACTORY = new DefaultDataBufferFactory();

    @Mock
    private KafkaStreamOperations kafkaStreamService;

    @Mock
    private WebSocketSession session;

    private KafkaStreamWebSocketHandler handler;
    private final List<String> outboundPayloads = new ArrayList<>();

    @BeforeEach
    void setUp() {
        handler = new KafkaStreamWebSocketHandler(kafkaStreamService, new ObjectMapper());
        outboundPayloads.clear();

        when(session.send(any())).thenAnswer(invocation -> {
            Flux<WebSocketMessage> outbound = invocation.getArgument(0);
            outbound.subscribe(message -> outboundPayloads.add(message.getPayloadAsText()));
            return reactor.core.publisher.Mono.empty();
        });
        when(session.textMessage(anyString())).thenAnswer(invocation ->
                textMessage((String) invocation.getArgument(0)));
    }

    @Test
    void handleStartActionStartsStreaming() {
        Disposable disposable = Disposables.single();
        when(kafkaStreamService.startStreaming(any(), any())).thenReturn(Flux.empty());
        when(kafkaStreamService.subscribe(any(), any())).thenReturn(disposable);
        when(session.receive()).thenReturn(Flux.just(textMessage("""
                {"action":"start","config":{"bootstrapServers":"localhost:9092","topic":"events","additionalProperties":""}}
                """)));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Streaming started"));
        verify(kafkaStreamService).startStreaming(any(), any());
        verify(kafkaStreamService).subscribe(any(), any());
    }

    @Test
    void handleStopActionStopsStreaming() {
        when(session.receive()).thenReturn(Flux.just(textMessage("{\"action\":\"stop\"}")));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Streaming stopped"));
    }

    @Test
    void handleInvalidJsonReturnsError() {
        when(session.receive()).thenReturn(Flux.just(textMessage("{invalid-json")));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Invalid message format"));
    }

    @Test
    void handleMissingActionReturnsError() {
        when(session.receive()).thenReturn(Flux.just(textMessage("{\"config\":{}}")));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Missing action"));
    }

    @Test
    void handleUnknownActionReturnsError() {
        when(session.receive()).thenReturn(Flux.just(textMessage("{\"action\":\"pause\"}")));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Unknown action"));
    }

    @Test
    void handleStartWithoutConfigReturnsError() {
        when(session.receive()).thenReturn(Flux.just(textMessage("{\"action\":\"start\"}")));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Config is required"));
    }

    @Test
    void handleStartWithoutBootstrapServersReturnsError() {
        when(session.receive()).thenReturn(Flux.just(textMessage("""
                {"action":"start","config":{"bootstrapServers":"","topic":"events","additionalProperties":""}}
                """)));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Bootstrap servers are required"));
    }

    @Test
    void handleStartWithoutTopicReturnsError() {
        when(session.receive()).thenReturn(Flux.just(textMessage("""
                {"action":"start","config":{"bootstrapServers":"localhost:9092","topic":"","additionalProperties":""}}
                """)));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(outboundPayloads).anyMatch(payload -> payload.contains("Topic name is required"));
    }

    private static WebSocketMessage textMessage(String payload) {
        DataBuffer buffer = BUFFER_FACTORY.wrap(payload.getBytes(StandardCharsets.UTF_8));
        return new WebSocketMessage(WebSocketMessage.Type.TEXT, buffer);
    }
}
