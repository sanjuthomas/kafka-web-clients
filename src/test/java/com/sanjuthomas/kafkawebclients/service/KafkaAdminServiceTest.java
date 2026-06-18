package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.ClusterConnectionRequest;
import com.sanjuthomas.kafkawebclients.model.ConsumerGroupOffsetInfo;
import com.sanjuthomas.kafkawebclients.model.CreateTopicRequest;
import com.sanjuthomas.kafkawebclients.model.PartitionOffsetInfo;
import com.sanjuthomas.kafkawebclients.model.ResetOffsetRequest;
import com.sanjuthomas.kafkawebclients.model.TopicAdminRequest;
import com.sanjuthomas.kafkawebclients.support.AdminClientFacade;
import com.sanjuthomas.kafkawebclients.support.AdminClientFacadeFactory;
import com.sanjuthomas.kafkawebclients.support.KafkaConfigSupport;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaAdminServiceTest {

    private FakeAdminClientFacade adminClientFacade;
    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        adminClientFacade = new FakeAdminClientFacade();
        AdminClientFacadeFactory factory = properties -> adminClientFacade;
        service = new KafkaAdminService(new KafkaConfigSupport(), factory);
    }

    @Test
    void listTopicsRequiresBootstrapServers() {
        StepVerifier.create(service.listTopics(new ClusterConnectionRequest("", "")))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Bootstrap servers are required"))
                .verifyComplete();
    }

    @Test
    void listTopicsReturnsUserTopics() {
        adminClientFacade.userTopics = List.of("events", "orders");

        StepVerifier.create(service.listTopics(new ClusterConnectionRequest("localhost:9092", "")))
                .assertNext(result -> {
                    assertThat(result.success()).isTrue();
                    assertThat(result.topics()).containsExactly("events", "orders");
                })
                .verifyComplete();
    }

    @Test
    void createTopicUsesDefaults() {
        StepVerifier.create(service.createTopic(new CreateTopicRequest(
                "localhost:9092", "", "new-topic", null, null)))
                .assertNext(result -> {
                    assertThat(result.success()).isTrue();
                    assertThat(adminClientFacade.lastCreatedTopic).isEqualTo("new-topic");
                    assertThat(adminClientFacade.lastCreatedPartitions).isEqualTo(1);
                    assertThat(adminClientFacade.lastCreatedReplicationFactor).isEqualTo((short) 1);
                })
                .verifyComplete();
    }

    @Test
    void createTopicMapsTopicExistsError() {
        adminClientFacade.createTopicException = new TopicExistsException("exists");

        StepVerifier.create(service.createTopic(new CreateTopicRequest(
                "localhost:9092", "", "events", 1, (short) 1)))
                .assertNext(result -> {
                    assertThat(result.success()).isFalse();
                    assertThat(result.error()).contains("already exists");
                })
                .verifyComplete();
    }

    @Test
    void deleteTopicRequiresTopicName() {
        StepVerifier.create(service.deleteTopic(new TopicAdminRequest("localhost:9092", "", "  ")))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Topic name is required"))
                .verifyComplete();
    }

    @Test
    void deleteTopicSucceeds() {
        StepVerifier.create(service.deleteTopic(new TopicAdminRequest("localhost:9092", "", "events")))
                .assertNext(result -> {
                    assertThat(result.success()).isTrue();
                    assertThat(adminClientFacade.lastDeletedTopic).isEqualTo("events");
                })
                .verifyComplete();
    }

    @Test
    void listConsumerGroupsReturnsOffsets() {
        adminClientFacade.consumerGroups = List.of(
                new ConsumerGroupOffsetInfo("group-a", List.of(new PartitionOffsetInfo(0, 12L))),
                new ConsumerGroupOffsetInfo("group-b", List.of(new PartitionOffsetInfo(1, 4L)))
        );

        StepVerifier.create(service.listConsumerGroups(new TopicAdminRequest("localhost:9092", "", "events")))
                .assertNext(result -> {
                    assertThat(result.success()).isTrue();
                    assertThat(result.groups()).hasSize(2);
                })
                .verifyComplete();
    }

    @Test
    void resetOffsetValidatesInput() {
        StepVerifier.create(service.resetOffset(new ResetOffsetRequest(
                "localhost:9092", "", "events", "", 0L)))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Consumer group is required"))
                .verifyComplete();

        StepVerifier.create(service.resetOffset(new ResetOffsetRequest(
                "localhost:9092", "", "events", "group-a", null)))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Offset is required"))
                .verifyComplete();
    }

    @Test
    void resetOffsetSucceeds() {
        StepVerifier.create(service.resetOffset(new ResetOffsetRequest(
                "localhost:9092", "", "events", "group-a", 0L)))
                .assertNext(result -> {
                    assertThat(result.success()).isTrue();
                    assertThat(adminClientFacade.lastResetGroup).isEqualTo("group-a");
                    assertThat(adminClientFacade.lastResetTopic).isEqualTo("events");
                    assertThat(adminClientFacade.lastResetOffset).isEqualTo(0L);
                })
                .verifyComplete();
    }

    @Test
    void resetOffsetMapsMissingGroup() {
        adminClientFacade.resetOffsetException = new GroupIdNotFoundException("missing");

        StepVerifier.create(service.resetOffset(new ResetOffsetRequest(
                "localhost:9092", "", "events", "missing", 0L)))
                .assertNext(result -> assertThat(result.error()).contains("was not found"))
                .verifyComplete();
    }

    @Test
    void deleteTopicMapsUnknownTopic() {
        adminClientFacade.deleteTopicException = new UnknownTopicOrPartitionException();

        StepVerifier.create(service.deleteTopic(new TopicAdminRequest("localhost:9092", "", "missing")))
                .assertNext(result -> assertThat(result.error()).contains("was not found"))
                .verifyComplete();
    }

    @Test
    void listTopicsClosesAdminClient() {
        AtomicBoolean closed = new AtomicBoolean(false);
        adminClientFacade.onClose = () -> closed.set(true);

        StepVerifier.create(service.listTopics(new ClusterConnectionRequest("localhost:9092", "")))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(closed).isTrue();
    }

    private static final class FakeAdminClientFacade implements AdminClientFacade {

        private List<String> userTopics = List.of();
        private List<ConsumerGroupOffsetInfo> consumerGroups = List.of();
        private String lastCreatedTopic;
        private int lastCreatedPartitions;
        private short lastCreatedReplicationFactor;
        private String lastDeletedTopic;
        private String lastResetGroup;
        private String lastResetTopic;
        private long lastResetOffset;
        private Exception createTopicException;
        private Exception deleteTopicException;
        private Exception resetOffsetException;
        private Runnable onClose = () -> {};

        @Override
        public String clusterId() {
            return "cluster-1";
        }

        @Override
        public int partitionCount(String topic) {
            return 1;
        }

        @Override
        public List<String> listUserTopics() throws Exception {
            if (createTopicException != null) {
                throw createTopicException;
            }
            return new ArrayList<>(userTopics);
        }

        @Override
        public void createTopic(String topic, int partitions, short replicationFactor) throws Exception {
            if (createTopicException != null) {
                throw createTopicException;
            }
            lastCreatedTopic = topic;
            lastCreatedPartitions = partitions;
            lastCreatedReplicationFactor = replicationFactor;
        }

        @Override
        public void deleteTopic(String topic) throws Exception {
            if (deleteTopicException != null) {
                throw deleteTopicException;
            }
            lastDeletedTopic = topic;
        }

        @Override
        public List<ConsumerGroupOffsetInfo> listConsumerGroupOffsetsForTopic(String topic) {
            return consumerGroups;
        }

        @Override
        public void resetConsumerGroupOffset(String consumerGroup, String topic, long offset) throws Exception {
            if (resetOffsetException != null) {
                throw resetOffsetException;
            }
            lastResetGroup = consumerGroup;
            lastResetTopic = topic;
            lastResetOffset = offset;
        }

        @Override
        public void close() {
            onClose.run();
        }
    }
}
