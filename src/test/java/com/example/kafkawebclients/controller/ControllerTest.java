package com.example.kafkawebclients.controller;

import com.example.kafkawebclients.model.ConfigValidationResult;
import com.example.kafkawebclients.model.ProduceRequest;
import com.example.kafkawebclients.model.ProduceResult;
import com.example.kafkawebclients.model.StreamConfig;
import com.example.kafkawebclients.service.KafkaConnectivityOperations;
import com.example.kafkawebclients.service.KafkaProducerOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigValidationControllerTest {

    @Mock
    private KafkaConnectivityOperations kafkaConnectivityService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new ConfigValidationController(kafkaConnectivityService))
                .build();
    }

    @Test
    void validateReturnsServiceResult() {
        when(kafkaConnectivityService.validate(any(StreamConfig.class)))
                .thenReturn(Mono.just(ConfigValidationResult.success("ok")));

        webTestClient.post()
                .uri("/api/config/validate")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"bootstrapServers":"localhost:9092","topic":"events","additionalProperties":""}
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.valid").isEqualTo(true)
                .jsonPath("$.message").isEqualTo("ok");
    }
}

@ExtendWith(MockitoExtension.class)
class ProducerControllerTest {

    @Mock
    private KafkaProducerOperations kafkaProducerService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new ProducerController(kafkaProducerService))
                .build();
    }

    @Test
    void produceReturnsServiceResult() {
        when(kafkaProducerService.produce(any(ProduceRequest.class)))
                .thenReturn(Mono.just(ProduceResult.success(0, 12L)));

        webTestClient.post()
                .uri("/api/produce")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {
                          "bootstrapServers":"localhost:9092",
                          "topic":"events",
                          "additionalProperties":"",
                          "key":"k1",
                          "payload":"hello"
                        }
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.success").isEqualTo(true)
                .jsonPath("$.offset").isEqualTo(12);
    }
}
