package com.sanjuthomas.kafkawebclients.support;

import org.springframework.stereotype.Component;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Component
public class DefaultKafkaSenderFactory implements KafkaSenderFactory {

    @Override
    public <K, V> KafkaSender<K, V> create(SenderOptions<K, V> options) {
        return KafkaSender.create(options);
    }
}
