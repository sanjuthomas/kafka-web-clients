package com.sanjuthomas.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConsumerGroupOffsetsResult(
        boolean success,
        List<ConsumerGroupOffsetInfo> groups,
        String message,
        String error
) {
    public static ConsumerGroupOffsetsResult success(List<ConsumerGroupOffsetInfo> groups) {
        return new ConsumerGroupOffsetsResult(
                true,
                groups,
                "Found " + groups.size() + " consumer group(s) with offsets for this topic.",
                null);
    }

    public static ConsumerGroupOffsetsResult failure(String error) {
        return new ConsumerGroupOffsetsResult(false, null, null, error);
    }
}
