package com.example.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ProduceRequest(
        String bootstrapServers,
        String topic,
        String additionalProperties,
        String key,
        String payload
) {
    public StreamConfig toStreamConfig() {
        return new StreamConfig(bootstrapServers, topic, additionalProperties, null);
    }
}
