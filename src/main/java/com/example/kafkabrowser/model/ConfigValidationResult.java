package com.example.kafkabrowser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConfigValidationResult(
        boolean valid,
        String message,
        String error
) {
    public static ConfigValidationResult success(String message) {
        return new ConfigValidationResult(true, message, null);
    }

    public static ConfigValidationResult failure(String error) {
        return new ConfigValidationResult(false, null, error);
    }
}
