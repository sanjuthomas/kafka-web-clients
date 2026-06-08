package com.example.kafkabrowser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record StreamConfig(
        String bootstrapServers,
        String topic,
        String additionalProperties
) {
}
