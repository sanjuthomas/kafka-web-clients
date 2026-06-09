package com.example.kafkawebclients.support;

import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DefaultAdminClientFacadeFactory implements AdminClientFacadeFactory {

    private final AdminClientFactory adminClientFactory;

    public DefaultAdminClientFacadeFactory(AdminClientFactory adminClientFactory) {
        this.adminClientFactory = adminClientFactory;
    }

    @Override
    public AdminClientFacade create(Map<String, Object> properties) {
        return new DefaultAdminClientFacade(adminClientFactory.create(properties));
    }
}
