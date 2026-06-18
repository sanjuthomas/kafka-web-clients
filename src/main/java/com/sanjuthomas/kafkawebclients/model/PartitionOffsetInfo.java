package com.sanjuthomas.kafkawebclients.model;

public record PartitionOffsetInfo(int partition, long offset) {
}
