package com.example.kafkawebclients.support;

import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@FunctionalInterface
public interface KafkaSenderFactory {

    <K, V> KafkaSender<K, V> create(SenderOptions<K, V> options);
}
