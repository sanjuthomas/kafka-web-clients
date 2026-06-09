package com.example.kafkawebclients.service;

import com.example.kafkawebclients.model.ProduceRequest;
import com.example.kafkawebclients.model.ProduceResult;
import com.example.kafkawebclients.support.KafkaConfigSupport;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Service
public class KafkaProducerService {

    private final KafkaConfigSupport kafkaConfigSupport;

    public KafkaProducerService(KafkaConfigSupport kafkaConfigSupport) {
        this.kafkaConfigSupport = kafkaConfigSupport;
    }

    public Mono<ProduceResult> produce(ProduceRequest request) {
        if (request == null) {
            return Mono.just(ProduceResult.failure("Produce request is required"));
        }

        if (request.bootstrapServers() == null || request.bootstrapServers().isBlank()) {
            return Mono.just(ProduceResult.failure("Bootstrap servers are required"));
        }

        if (request.topic() == null || request.topic().isBlank()) {
            return Mono.just(ProduceResult.failure("Topic name is required"));
        }

        if (request.payload() == null || request.payload().isBlank()) {
            return Mono.just(ProduceResult.failure("Message payload is required"));
        }

        String topic = request.topic().trim();
        String key = request.key() == null || request.key().isBlank() ? null : request.key().trim();
        SenderOptions<String, String> senderOptions = SenderOptions.create(
                kafkaConfigSupport.buildProducerProperties(request.toStreamConfig()));

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, request.payload());
        SenderRecord<String, String, String> senderRecord = SenderRecord.create(producerRecord, "browser-produce");

        return Mono.fromCallable(() -> {
                    KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
                    try {
                        var result = sender.send(Mono.just(senderRecord)).blockFirst();
                        if (result == null) {
                            return ProduceResult.failure("No response received from Kafka producer");
                        }
                        return ProduceResult.success(
                                result.recordMetadata().partition(),
                                result.recordMetadata().offset());
                    } finally {
                        sender.close();
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> Mono.just(ProduceResult.failure(
                        error.getMessage() != null ? error.getMessage() : "Failed to send message")));
    }
}
