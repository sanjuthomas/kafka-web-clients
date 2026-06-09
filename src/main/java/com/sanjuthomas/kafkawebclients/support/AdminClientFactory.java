package com.sanjuthomas.kafkawebclients.support;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Map;

@FunctionalInterface
public interface AdminClientFactory {

    AdminClient create(Map<String, Object> properties);
}
