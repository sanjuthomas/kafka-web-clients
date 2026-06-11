package com.sanjuthomas.kafkawebclients.support;

import org.apache.kafka.clients.admin.Admin;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DefaultAdminClientFacade implements AdminClientFacade {

    private static final int CONNECT_TIMEOUT_SECONDS = 10;

    private final Admin admin;

    public DefaultAdminClientFacade(Admin admin) {
        this.admin = admin;
    }

    @Override
    public String clusterId() throws Exception {
        return admin.describeCluster()
                .clusterId()
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public int partitionCount(String topic) throws Exception {
        var topicDescription = admin.describeTopics(Collections.singletonList(topic))
                .topicNameValues()
                .get(topic)
                .get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return topicDescription.partitions().size();
    }

    @Override
    public void close() {
        admin.close();
    }
}
