package com.sanjuthomas.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResetOffsetRequest(
        String bootstrapServers,
        String additionalProperties,
        String topic,
        String consumerGroup,
        Long offset
) {
}
