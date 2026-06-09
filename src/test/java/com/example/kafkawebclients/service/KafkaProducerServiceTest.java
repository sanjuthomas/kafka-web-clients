package com.example.kafkawebclients.service;

import com.example.kafkawebclients.model.ProduceRequest;
import com.example.kafkawebclients.support.KafkaConfigSupport;
import com.example.kafkawebclients.support.KafkaSenderFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaSenderFactory kafkaSenderFactory;

    @Mock
    private KafkaSender<String, String> sender;

    private KafkaProducerService service;

    @BeforeEach
    void setUp() {
        service = new KafkaProducerService(new KafkaConfigSupport(), kafkaSenderFactory);
    }

    @Test
    void produceRejectsInvalidRequests() {
        StepVerifier.create(service.produce(null))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Produce request is required"))
                .verifyComplete();

        StepVerifier.create(service.produce(new ProduceRequest("", "topic", "", null, "payload")))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Bootstrap servers are required"))
                .verifyComplete();

        StepVerifier.create(service.produce(new ProduceRequest("localhost:9092", "", "", null, "payload")))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Topic name is required"))
                .verifyComplete();

        StepVerifier.create(service.produce(new ProduceRequest("localhost:9092", "events", "", null, "  ")))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Message payload is required"))
                .verifyComplete();
    }

    @Test
    void produceReturnsMetadataWhenSendSucceeds() {
        ProduceRequest request = new ProduceRequest("localhost:9092", "events", "", "user-1", "hello");
        SenderResult<String> senderResult = mock(SenderResult.class);
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("events", 0), 0L, 0, 0L, 0, 0);

        when(kafkaSenderFactory.create(any())).thenAnswer(invocation -> sender);
        when(sender.send(any())).thenAnswer(invocation -> Flux.just(senderResult));
        when(senderResult.recordMetadata()).thenReturn(metadata);

        StepVerifier.create(service.produce(request))
                .assertNext(result -> {
                    assertThat(result.success()).isTrue();
                    assertThat(result.partition()).isEqualTo(0);
                    assertThat(result.offset()).isEqualTo(0L);
                })
                .verifyComplete();
    }

    @Test
    void produceReturnsFailureWhenSenderReturnsNoResult() {
        ProduceRequest request = new ProduceRequest("localhost:9092", "events", "", null, "hello");

        when(kafkaSenderFactory.create(any())).thenAnswer(invocation -> sender);
        when(sender.send(any())).thenAnswer(invocation -> Flux.empty());

        StepVerifier.create(service.produce(request))
                .assertNext(result -> assertThat(result.error()).contains("No response received"))
                .verifyComplete();
    }

    @Test
    void produceMapsSendErrors() {
        ProduceRequest request = new ProduceRequest("localhost:9092", "events", "", null, "hello");

        when(kafkaSenderFactory.create(any())).thenAnswer(invocation -> sender);
        when(sender.send(any())).thenAnswer(invocation -> Flux.error(new RuntimeException("broker down")));

        StepVerifier.create(service.produce(request))
                .assertNext(result -> assertThat(result.error()).isEqualTo("broker down"))
                .verifyComplete();
    }
}
