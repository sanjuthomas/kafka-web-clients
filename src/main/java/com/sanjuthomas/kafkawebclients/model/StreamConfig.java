package com.sanjuthomas.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record StreamConfig(
        String bootstrapServers,
        String topic,
        String additionalProperties,
        String autoOffsetReset
) {
}
