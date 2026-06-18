package com.sanjuthomas.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TopicListResult(
        boolean success,
        List<String> topics,
        String message,
        String error
) {
    public static TopicListResult success(List<String> topics) {
        return new TopicListResult(
                true,
                topics,
                "Found " + topics.size() + " user topic(s).",
                null);
    }

    public static TopicListResult failure(String error) {
        return new TopicListResult(false, null, null, error);
    }
}
