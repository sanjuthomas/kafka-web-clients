package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import com.sanjuthomas.kafkawebclients.model.WebSocketMessage;
import com.sanjuthomas.kafkawebclients.support.KafkaConfigSupport;
import com.sanjuthomas.kafkawebclients.support.KafkaReceiverFactory;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

@Service
public class KafkaStreamService implements KafkaStreamOperations {

    private static final Duration COMMIT_RETRY_INTERVAL = Duration.ofSeconds(1);
    private static final int MAX_COMMIT_ATTEMPTS = 3_600;
    private static final Duration STREAM_RETRY_MIN_BACKOFF = Duration.ofSeconds(1);
    private static final Duration STREAM_RETRY_MAX_BACKOFF = Duration.ofSeconds(30);

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
                .commitRetryInterval(COMMIT_RETRY_INTERVAL)
                .maxCommitAttempts(MAX_COMMIT_ATTEMPTS)
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

        return Flux.defer(receiver::receive)
                .doOnSubscribe(sub -> controlSink.tryEmitNext(WebSocketMessage.status(
                        "Connected to Kafka. Consuming from "
                                + kafkaConfigSupport.resolveAutoOffsetReset(config)
                                + " on topic '" + config.topic() + "'...")))
                .map(this::toWebSocketMessage)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, STREAM_RETRY_MIN_BACKOFF)
                        .maxBackoff(STREAM_RETRY_MAX_BACKOFF)
                        .filter(this::isRetriableKafkaError)
                        .doBeforeRetry(signal -> controlSink.tryEmitNext(WebSocketMessage.status(
                                "Kafka unavailable — reconnecting (attempt "
                                        + (signal.totalRetries() + 1) + ")..."))))
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

    private boolean isRetriableKafkaError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof RetriableCommitFailedException || current instanceof RetriableException) {
                return true;
            }

            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase();
                if (lower.contains("connection refused")
                        || lower.contains("connection timed out")
                        || lower.contains("disconnected")
                        || lower.contains("broker may not be available")
                        || lower.contains("node may not be available")
                        || lower.contains("timed out waiting for a node assignment")) {
                    return true;
                }
            }

            current = current.getCause();
        }
        return false;
    }
}
