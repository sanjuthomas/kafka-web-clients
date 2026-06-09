package com.sanjuthomas.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProduceResult(
        boolean success,
        String message,
        Integer partition,
        Long offset,
        String error
) {
    public static ProduceResult success(int partition, long offset) {
        return new ProduceResult(
                true,
                "Message sent to partition " + partition + " at offset " + offset + ".",
                partition,
                offset,
                null);
    }

    public static ProduceResult failure(String error) {
        return new ProduceResult(false, null, null, null, error);
    }
}
