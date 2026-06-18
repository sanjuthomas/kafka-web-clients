package com.sanjuthomas.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AdminResult(
        boolean success,
        String message,
        String error
) {
    public static AdminResult success(String message) {
        return new AdminResult(true, message, null);
    }

    public static AdminResult failure(String error) {
        return new AdminResult(false, null, error);
    }
}
