package com.sanjuthomas.kafkawebclients.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConsumerGroupOffsetInfo(
        String groupId,
        List<PartitionOffsetInfo> partitions
) {
}
