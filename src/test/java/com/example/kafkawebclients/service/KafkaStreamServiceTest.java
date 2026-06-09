package com.example.kafkawebclients.service;

import com.example.kafkawebclients.model.StreamConfig;
import com.example.kafkawebclients.model.WebSocketMessage;
import com.example.kafkawebclients.support.KafkaConfigSupport;
import com.example.kafkawebclients.support.KafkaReceiverFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaStreamServiceTest {

    @Mock
    private KafkaReceiverFactory kafkaReceiverFactory;

    @Mock
    private KafkaReceiver<String, String> receiver;

    private KafkaStreamService service;

    @BeforeEach
    void setUp() {
        service = new KafkaStreamService(new KafkaConfigSupport(), kafkaReceiverFactory);
    }

    @Test
    void startStreamingMapsRecordsAndStatusMessages() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", "latest");
        Sinks.Many<WebSocketMessage> controlSink = Sinks.many().unicast().onBackpressureBuffer();
        AtomicBoolean acknowledged = new AtomicBoolean(false);
        ReceiverRecord<String, String> receiverRecord = receiverRecord("key-1", "payload-1", acknowledged);

        when(kafkaReceiverFactory.create(any())).thenAnswer(invocation -> receiver);
        when(receiver.receive()).thenReturn(Flux.just(receiverRecord));

        StepVerifier.create(service.startStreaming(config, controlSink))
                .assertNext(message -> {
                    assertThat(message.type()).isEqualTo("record");
                    assertThat(message.key()).isEqualTo("key-1");
                    assertThat(message.payload()).isEqualTo("payload-1");
                })
                .verifyComplete();

        assertThat(acknowledged).isTrue();
    }

    @Test
    void startStreamingMapsReceiverErrors() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", "latest");
        Sinks.Many<WebSocketMessage> controlSink = Sinks.many().unicast().onBackpressureBuffer();
        when(kafkaReceiverFactory.create(any())).thenAnswer(invocation -> receiver);
        when(receiver.receive()).thenReturn(Flux.error(new RuntimeException("stream failed")));

        StepVerifier.create(service.startStreaming(config, controlSink))
                .assertNext(message -> {
                    assertThat(message.type()).isEqualTo("error");
                    assertThat(message.error()).isEqualTo("stream failed");
                })
                .verifyComplete();
    }

    @Test
    void startStreamingSeeksToBeginningWhenConfigured() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", "earliest");
        Sinks.Many<WebSocketMessage> controlSink = Sinks.many().unicast().onBackpressureBuffer();
        List<ReceiverOptions<String, String>> capturedOptions = new ArrayList<>();
        AtomicBoolean seeked = new AtomicBoolean(false);

        when(kafkaReceiverFactory.create(any())).thenAnswer(invocation -> {
            capturedOptions.add(invocation.getArgument(0));
            return receiver;
        });
        when(receiver.receive()).thenReturn(Flux.empty());

        StepVerifier.create(service.startStreaming(config, controlSink)).verifyComplete();

        assertThat(capturedOptions).hasSize(1);
        ReceiverOptions<String, String> options = capturedOptions.getFirst();
        options.assignListeners().forEach(listener -> listener.accept(List.of(new FakeReceiverPartition(seeked))));

        assertThat(seeked).isTrue();
    }

    @Test
    void subscribeForwardsMessagesAndErrors() {
        Sinks.Many<WebSocketMessage> outputSink = Sinks.many().unicast().onBackpressureBuffer();
        Flux<WebSocketMessage> stream = Flux.concat(
                Flux.just(WebSocketMessage.status("ok")),
                Flux.error(new RuntimeException("subscription failed")));

        service.subscribe(stream, outputSink);

        StepVerifier.create(outputSink.asFlux().take(2))
                .assertNext(message -> assertThat(message.payload()).isEqualTo("ok"))
                .assertNext(message -> assertThat(message.error()).isEqualTo("subscription failed"))
                .verifyComplete();
    }

    private static ReceiverRecord<String, String> receiverRecord(
            String key,
            String value,
            AtomicBoolean acknowledged
    ) {
        ConsumerRecord<String, String> consumerRecord =
                new ConsumerRecord<>("events", 0, 15L, key, value);
        return new ReceiverRecord<>(consumerRecord, new FakeReceiverOffset(acknowledged));
    }

    private static final class FakeReceiverOffset implements reactor.kafka.receiver.ReceiverOffset {

        private final AtomicBoolean acknowledged;

        private FakeReceiverOffset(AtomicBoolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        @Override
        public TopicPartition topicPartition() {
            return new TopicPartition("events", 0);
        }

        @Override
        public long offset() {
            return 15L;
        }

        @Override
        public void acknowledge() {
            acknowledged.set(true);
        }

        @Override
        public reactor.core.publisher.Mono<Void> commit() {
            return reactor.core.publisher.Mono.empty();
        }
    }

    private static final class FakeReceiverPartition implements ReceiverPartition {

        private final AtomicBoolean seeked;

        private FakeReceiverPartition(AtomicBoolean seeked) {
            this.seeked = seeked;
        }

        @Override
        public TopicPartition topicPartition() {
            return new TopicPartition("events", 0);
        }

        @Override
        public void seekToBeginning() {
            seeked.set(true);
        }

        @Override
        public void seekToEnd() {
        }

        @Override
        public void seek(long offset) {
        }

        @Override
        public void seekToTimestamp(long timestamp) {
        }

        @Override
        public long position() {
            return 0L;
        }
    }
}
