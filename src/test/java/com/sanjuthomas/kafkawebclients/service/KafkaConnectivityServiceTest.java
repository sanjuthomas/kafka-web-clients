package com.sanjuthomas.kafkawebclients.service;

import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import com.sanjuthomas.kafkawebclients.support.AdminClientFacade;
import com.sanjuthomas.kafkawebclients.support.AdminClientFacadeFactory;
import com.sanjuthomas.kafkawebclients.support.KafkaConfigSupport;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConnectivityServiceTest {

    private FakeAdminClientFacade adminClientFacade;
    private KafkaConnectivityService service;

    @BeforeEach
    void setUp() {
        adminClientFacade = new FakeAdminClientFacade();
        AdminClientFacadeFactory factory = properties -> adminClientFacade;
        service = new KafkaConnectivityService(new KafkaConfigSupport(), factory);
    }

    @Test
    void validateRejectsMissingInput() {
        StepVerifier.create(service.validate(null))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Configuration is required"))
                .verifyComplete();

        StepVerifier.create(service.validate(new StreamConfig("", "topic", "", null, null)))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Bootstrap servers are required"))
                .verifyComplete();

        StepVerifier.create(service.validate(new StreamConfig("localhost:9092", "  ", "", null, null)))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Topic name is required"))
                .verifyComplete();
    }

    @Test
    void validateReturnsSuccessWhenTopicExists() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.partitionCount = 2;

        StepVerifier.create(service.validate(config))
                .assertNext(result -> {
                    assertThat(result.valid()).isTrue();
                    assertThat(result.message()).contains("events").contains("2 partition");
                })
                .verifyComplete();
    }

    @Test
    void validateMapsUnknownTopicToFriendlyError() {
        StreamConfig config = new StreamConfig("localhost:9092", "missing", "", null, null);
        adminClientFacade.partitionCountException = new ExecutionException(new UnknownTopicOrPartitionException());

        StepVerifier.create(service.validate(config))
                .assertNext(result -> {
                    assertThat(result.valid()).isFalse();
                    assertThat(result.error()).contains("missing").contains("was not found");
                })
                .verifyComplete();
    }

    @Test
    void validateMapsTimeoutToFriendlyError() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.clusterIdException = new TimeoutException();

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("did not respond within 10 seconds"))
                .verifyComplete();
    }

    @Test
    void validateMapsAuthenticationFailures() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.partitionCountException = new ExecutionException(new AuthenticationException("auth failed"));

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("authentication failed"))
                .verifyComplete();
    }

    @Test
    void validateMapsConnectionRefusedMessage() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.partitionCountException = new ExecutionException(
                new RuntimeException("Connection to node -1 could not be established"));

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("broker is running and reachable"))
                .verifyComplete();
    }

    @Test
    void validateMapsInterruptedException() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.partitionCountException = new InterruptedException();

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).isEqualTo("Kafka connectivity check was interrupted"))
                .verifyComplete();
    }

    @Test
    void validateMapsTopicAuthorizationFailures() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.partitionCountException = new ExecutionException(new TopicAuthorizationException("denied"));

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("Not authorized to access topic"))
                .verifyComplete();
    }

    @Test
    void validateMapsClusterAuthorizationFailures() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.clusterIdException = new ExecutionException(new ClusterAuthorizationException("denied"));

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("Not authorized to connect"))
                .verifyComplete();
    }

    @Test
    void validateMapsInvalidTopicFailures() {
        StreamConfig config = new StreamConfig("localhost:9092", "bad topic", "", null, null);
        adminClientFacade.partitionCountException = new ExecutionException(new InvalidTopicException("bad topic"));

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("Invalid topic name"))
                .verifyComplete();
    }

    @Test
    void validateMapsUnknownHostFailures() {
        StreamConfig config = new StreamConfig("unknown-host:9092", "events", "", null, null);
        adminClientFacade.clusterIdException = new UnknownHostException("unknown-host");

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("Could not resolve a Kafka host"))
                .verifyComplete();
    }

    @Test
    void validateMapsConnectExceptionFailures() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.clusterIdException = new ConnectException("connection refused");

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error())
                        .containsIgnoringCase("could not connect")
                        .containsIgnoringCase("unreachable"))
                .verifyComplete();
    }

    @Test
    void validateMapsSslFailuresFromMessage() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        adminClientFacade.clusterIdException = new RuntimeException("SSL handshake failed");

        StepVerifier.create(service.validate(config))
                .assertNext(result -> assertThat(result.error()).contains("SSL/TLS connection failed"))
                .verifyComplete();
    }

    @Test
    void validateClosesAdminClientFacade() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);
        AtomicBoolean closed = new AtomicBoolean(false);
        adminClientFacade.onClose = () -> closed.set(true);

        StepVerifier.create(service.validate(config))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(closed).isTrue();
    }

    private static final class FakeAdminClientFacade implements AdminClientFacade {

        private int partitionCount = 1;
        private Exception clusterIdException;
        private Exception partitionCountException;
        private Runnable onClose = () -> {};

        @Override
        public String clusterId() throws Exception {
            if (clusterIdException != null) {
                throw clusterIdException;
            }
            return "cluster-1";
        }

        @Override
        public int partitionCount(String topic) throws Exception {
            if (partitionCountException != null) {
                throw partitionCountException;
            }
            return partitionCount;
        }

        @Override
        public void close() {
            onClose.run();
        }
    }
}
