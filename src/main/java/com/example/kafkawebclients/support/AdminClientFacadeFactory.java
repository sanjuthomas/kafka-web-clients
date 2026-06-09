package com.example.kafkawebclients.support;

import java.util.Map;

@FunctionalInterface
public interface AdminClientFacadeFactory {

    AdminClientFacade create(Map<String, Object> properties);
}
