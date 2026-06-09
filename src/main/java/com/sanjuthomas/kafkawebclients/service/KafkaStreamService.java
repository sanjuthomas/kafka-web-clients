package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import com.sanjuthomas.kafkawebclients.model.WebSocketMessage;
import com.sanjuthomas.kafkawebclients.support.KafkaConfigSupport;
import com.sanjuthomas.kafkawebclients.support.KafkaReceiverFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.Map;

@Service
public class KafkaStreamService implements KafkaStreamOperations {

    private final KafkaConfigSupport kafkaConfigSupport;
    private final KafkaReceiverFactory kafkaReceiverFactory;

    public KafkaStreamService(KafkaConfigSupport kafkaConfigSupport, KafkaReceiverFactory kafkaReceiverFactory) {
        this.kafkaConfigSupport = kafkaConfigSupport;
        this.kafkaReceiverFactory = kafkaReceiverFactory;
    }

    public Flux<WebSocketMessage> startStreaming(StreamConfig config, Sinks.Many<WebSocketMessage> controlSink) {
        Map<String, Object> consumerProps = kafkaConfigSupport.buildConsumerProperties(config);
        boolean startFromEarliest = "earliest".equals(kafkaConfigSupport.resolveAutoOffsetReset(config));

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(config.topic()))
                .addAssignListener(partitions -> {
                    if (startFromEarliest) {
                        partitions.forEach(ReceiverPartition::seekToBeginning);
                    }
                    controlSink.tryEmitNext(WebSocketMessage.status(
                            "Assigned partitions: " + partitions.size()
                                    + (startFromEarliest ? " (starting from earliest offset)" : "")));
                })
                .addRevokeListener(partitions ->
                        controlSink.tryEmitNext(WebSocketMessage.status(
                                "Revoked partitions: " + partitions.size())));

        KafkaReceiver<String, String> receiver = kafkaReceiverFactory.create(receiverOptions);

        return receiver.receive()
                .doOnSubscribe(sub -> controlSink.tryEmitNext(WebSocketMessage.status(
                        "Connected to Kafka. Consuming from "
                                + kafkaConfigSupport.resolveAutoOffsetReset(config)
                                + " on topic '" + config.topic() + "'...")))
                .map(this::toWebSocketMessage)
                .onErrorResume(error -> Flux.just(WebSocketMessage.error(error.getMessage())));
    }

    public Disposable subscribe(Flux<WebSocketMessage> stream, Sinks.Many<WebSocketMessage> outputSink) {
        return stream.subscribe(
                outputSink::tryEmitNext,
                error -> outputSink.tryEmitNext(WebSocketMessage.error(error.getMessage()))
        );
    }

    private WebSocketMessage toWebSocketMessage(ReceiverRecord<String, String> record) {
        record.receiverOffset().acknowledge();
        return WebSocketMessage.record(
                record.key(),
                record.value(),
                record.partition(),
                record.offset(),
                record.timestamp()
        );
    }
}
