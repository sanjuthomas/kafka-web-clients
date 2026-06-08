package com.example.kafkabrowser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WebSocketMessage(
        String action,
        StreamConfig config,
        String type,
        String payload,
        String key,
        Integer partition,
        Long offset,
        Long timestamp,
        String error
) {
    public static WebSocketMessage status(String message) {
        return new WebSocketMessage(null, null, "status", message, null, null, null, null, null);
    }

    public static WebSocketMessage record(String key, String payload, int partition, long offset, long timestamp) {
        return new WebSocketMessage(null, null, "record", payload, key, partition, offset, timestamp, null);
    }

    public static WebSocketMessage error(String message) {
        return new WebSocketMessage(null, null, "error", null, null, null, null, null, message);
    }
}
