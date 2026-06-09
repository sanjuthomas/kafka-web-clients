package com.example.kafkawebclients.support;

import com.example.kafkawebclients.model.StreamConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Component
public class KafkaConfigSupport {

    public Map<String, Object> buildConsumerProperties(StreamConfig config) {
        Map<String, Object> props = buildBaseProperties(config);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-web-clients-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public Map<String, Object> buildProducerProperties(StreamConfig config) {
        Map<String, Object> props = buildBaseProperties(config);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-web-clients-producer-" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    public Map<String, Object> buildAdminProperties(StreamConfig config) {
        Map<String, Object> props = buildBaseProperties(config);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-web-clients-connectivity-" + UUID.randomUUID());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
        return props;
    }

    private Map<String, Object> buildBaseProperties(StreamConfig config) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers().trim());
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
