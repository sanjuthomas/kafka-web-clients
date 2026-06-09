package com.sanjuthomas.kafkawebclients.support;

public interface AdminClientFacade extends AutoCloseable {

    String clusterId() throws Exception;

    int partitionCount(String topic) throws Exception;

    @Override
    void close();
}
