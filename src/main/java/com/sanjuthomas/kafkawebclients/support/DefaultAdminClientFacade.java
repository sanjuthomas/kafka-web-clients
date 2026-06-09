package com.sanjuthomas.kafkawebclients.support;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DefaultAdminClientFacade implements AdminClientFacade {

    private static final int CONNECT_TIMEOUT_SECONDS = 10;

    private final AdminClient adminClient;

    public DefaultAdminClientFacade(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public String clusterId() throws Exception {
        return adminClient.describeCluster()
                .clusterId()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public int partitionCount(String topic) throws Exception {
        var topicDescription = adminClient.describeTopics(Collections.singletonList(topic))
                .topicNameValues()
                .get(topic)
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return topicDescription.partitions().size();
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
