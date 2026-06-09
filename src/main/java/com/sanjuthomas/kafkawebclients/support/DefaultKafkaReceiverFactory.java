package com.sanjuthomas.kafkawebclients.support;

import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@Component
public class DefaultKafkaReceiverFactory implements KafkaReceiverFactory {

    @Override
    public <K, V> KafkaReceiver<K, V> create(ReceiverOptions<K, V> options) {
        return KafkaReceiver.create(options);
    }
}
