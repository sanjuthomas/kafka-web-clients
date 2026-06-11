package com.sanjuthomas.kafkawebclients.config;

import com.sanjuthomas.kafkawebclients.handler.KafkaStreamWebSocketHandler;
import com.sanjuthomas.kafkawebclients.service.KafkaStreamOperations;
import com.sanjuthomas.kafkawebclients.support.KafkaConfigSupport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.config.ResourceHandlerRegistry;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WebSocketConfigTest {

    private final WebSocketConfig config = new WebSocketConfig();

    @Test
    void webSocketHandlerMappingRegistersStreamEndpoint() {
        KafkaStreamOperations streamOperations = new KafkaStreamOperations() {
            @Override
            public Flux<com.sanjuthomas.kafkawebclients.model.WebSocketMessage> startStreaming(
                    com.sanjuthomas.kafkawebclients.model.StreamConfig config,
                    reactor.core.publisher.Sinks.Many<com.sanjuthomas.kafkawebclients.model.WebSocketMessage> controlSink
            ) {
                return Flux.empty();
            }

            @Override
            public reactor.core.Disposable subscribe(
                    Flux<com.sanjuthomas.kafkawebclients.model.WebSocketMessage> stream,
                    reactor.core.publisher.Sinks.Many<com.sanjuthomas.kafkawebclients.model.WebSocketMessage> outputSink
            ) {
                return Disposables.single();
            }
        };
        KafkaStreamWebSocketHandler handler = new KafkaStreamWebSocketHandler(
                streamOperations, new KafkaConfigSupport(), new ObjectMapper());

        SimpleUrlHandlerMapping mapping = (SimpleUrlHandlerMapping) config.webSocketHandlerMapping(handler);
        @SuppressWarnings("unchecked")
        Map<String, Object> urlMap = (Map<String, Object>) ReflectionTestUtils.getField(mapping, "urlMap");

        assertThat(urlMap).containsKey("/ws/stream");
        assertThat(urlMap.get("/ws/stream")).isSameAs(handler);
        assertThat(mapping.getOrder()).isEqualTo(-1);
    }

    @Test
    void handlerAdapterBeanIsCreated() {
        WebSocketHandlerAdapter adapter = config.handlerAdapter();

        assertThat(adapter).isNotNull();
    }

    @Test
    void addResourceHandlersRegistersClasspathStaticLocation() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.refresh();

        ResourceHandlerRegistry registry = new ResourceHandlerRegistry(context);
        config.addResourceHandlers(registry);

        assertThat(registry.hasMappingForPattern("/**")).isTrue();
    }
}
