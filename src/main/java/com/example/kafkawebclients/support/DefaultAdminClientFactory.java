package com.example.kafkawebclients.support;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DefaultAdminClientFactory implements AdminClientFactory {

    @Override
    public AdminClient create(Map<String, Object> properties) {
        return AdminClient.create(properties);
    }
}
