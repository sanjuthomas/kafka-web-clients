package com.example.kafkabrowser.service;

import com.example.kafkabrowser.model.StreamConfig;
import com.example.kafkabrowser.model.WebSocketMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Service
public class KafkaStreamService {

    public Flux<WebSocketMessage> startStreaming(StreamConfig config, Sinks.Many<WebSocketMessage> controlSink) {
        Map<String, Object> consumerProps = buildConsumerProperties(config);

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(config.topic()))
                .addAssignListener(partitions ->
                        controlSink.tryEmitNext(WebSocketMessage.status(
                                "Assigned partitions: " + partitions.size())))
                .addRevokeListener(partitions ->
                        controlSink.tryEmitNext(WebSocketMessage.status(
                                "Revoked partitions: " + partitions.size())));

        KafkaReceiver<String, String> receiver = KafkaReceiver.create(receiverOptions);

        return receiver.receive()
                .doOnSubscribe(sub -> controlSink.tryEmitNext(WebSocketMessage.status(
                        "Connected to Kafka. Waiting for messages on topic '" + config.topic() + "'...")))
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

    private Map<String, Object> buildConsumerProperties(StreamConfig config) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers().trim());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-browser-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        applyAdditionalProperties(props, config.additionalProperties());
        return props;
    }

    private void applyAdditionalProperties(Map<String, Object> props, String additionalProperties) {
        if (additionalProperties == null || additionalProperties.isBlank()) {
            return;
        }

        Properties parsed = new Properties();
        for (String line : additionalProperties.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            int separator = trimmed.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            String key = trimmed.substring(0, separator).trim();
            String value = trimmed.substring(separator + 1).trim();
            if (!key.isEmpty()) {
                parsed.setProperty(key, value);
            }
        }

        for (String name : parsed.stringPropertyNames()) {
            props.put(name, parsed.getProperty(name));
        }
    }
}
