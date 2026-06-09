package com.example.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record StreamConfig(
        String bootstrapServers,
        String topic,
        String additionalProperties
) {
}
