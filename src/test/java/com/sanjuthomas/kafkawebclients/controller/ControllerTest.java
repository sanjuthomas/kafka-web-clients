package com.sanjuthomas.kafkawebclients.controller;

import com.sanjuthomas.kafkawebclients.model.AdminResult;
import com.sanjuthomas.kafkawebclients.model.ClusterConnectionRequest;
import com.sanjuthomas.kafkawebclients.model.ConfigValidationResult;
import com.sanjuthomas.kafkawebclients.model.ProduceRequest;
import com.sanjuthomas.kafkawebclients.model.ProduceResult;
import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import com.sanjuthomas.kafkawebclients.model.TopicListResult;
import com.sanjuthomas.kafkawebclients.service.KafkaAdminOperations;
import com.sanjuthomas.kafkawebclients.service.KafkaConnectivityOperations;
import com.sanjuthomas.kafkawebclients.service.KafkaProducerOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;

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

    @Test
    void validateClusterReturnsServiceResult() {
        when(kafkaConnectivityService.validateCluster(any(ClusterConnectionRequest.class)))
                .thenReturn(Mono.just(ConfigValidationResult.success("connected")));

        webTestClient.post()
                .uri("/api/config/validate-cluster")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"bootstrapServers":"localhost:9092","additionalProperties":""}
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.valid").isEqualTo(true)
                .jsonPath("$.message").isEqualTo("connected");
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

@ExtendWith(MockitoExtension.class)
class AdminControllerTest {

    @Mock
    private KafkaAdminOperations kafkaAdminOperations;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new AdminController(kafkaAdminOperations))
                .build();
    }

    @Test
    void listTopicsReturnsServiceResult() {
        when(kafkaAdminOperations.listTopics(any(ClusterConnectionRequest.class)))
                .thenReturn(Mono.just(TopicListResult.success(List.of("events"))));

        webTestClient.post()
                .uri("/api/admin/topics/list")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"bootstrapServers":"localhost:9092","additionalProperties":""}
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.success").isEqualTo(true)
                .jsonPath("$.topics[0]").isEqualTo("events");
    }

    @Test
    void resetOffsetReturnsServiceResult() {
        when(kafkaAdminOperations.resetOffset(any()))
                .thenReturn(Mono.just(AdminResult.success("reset ok")));

        webTestClient.post()
                .uri("/api/admin/consumer-groups/reset-offset")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {
                          "bootstrapServers":"localhost:9092",
                          "additionalProperties":"",
                          "topic":"events",
                          "consumerGroup":"group-a",
                          "offset":0
                        }
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.success").isEqualTo(true)
                .jsonPath("$.message").isEqualTo("reset ok");
    }
}
