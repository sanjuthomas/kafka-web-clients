package com.example.kafkawebclients.support;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@FunctionalInterface
public interface KafkaReceiverFactory {

    <K, V> KafkaReceiver<K, V> create(ReceiverOptions<K, V> options);
}
