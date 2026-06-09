package com.example.kafkawebclients.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ModelFactoryTest {

    @Test
    void webSocketMessageFactoriesPopulateExpectedFields() {
        WebSocketMessage status = WebSocketMessage.status("ready");
        WebSocketMessage record = WebSocketMessage.record("key", "payload", 1, 42L, 100L);
        WebSocketMessage error = WebSocketMessage.error("boom");

        assertThat(status.type()).isEqualTo("status");
        assertThat(status.payload()).isEqualTo("ready");
        assertThat(record.key()).isEqualTo("key");
        assertThat(record.partition()).isEqualTo(1);
        assertThat(record.offset()).isEqualTo(42L);
        assertThat(error.error()).isEqualTo("boom");
    }

    @Test
    void configValidationResultFactories() {
        ConfigValidationResult success = ConfigValidationResult.success("ok");
        ConfigValidationResult failure = ConfigValidationResult.failure("bad");

        assertThat(success.valid()).isTrue();
        assertThat(success.message()).isEqualTo("ok");
        assertThat(failure.valid()).isFalse();
        assertThat(failure.error()).isEqualTo("bad");
    }

    @Test
    void produceResultFactories() {
        ProduceResult success = ProduceResult.success(0, 10L);
        ProduceResult failure = ProduceResult.failure("failed");

        assertThat(success.success()).isTrue();
        assertThat(success.partition()).isEqualTo(0);
        assertThat(success.offset()).isEqualTo(10L);
        assertThat(failure.success()).isFalse();
        assertThat(failure.error()).isEqualTo("failed");
    }

    @Test
    void produceRequestMapsToStreamConfig() {
        ProduceRequest request = new ProduceRequest("localhost:9092", "events", "a=b", "key", "payload");

        StreamConfig config = request.toStreamConfig();

        assertThat(config.bootstrapServers()).isEqualTo("localhost:9092");
        assertThat(config.topic()).isEqualTo("events");
        assertThat(config.additionalProperties()).isEqualTo("a=b");
        assertThat(config.autoOffsetReset()).isNull();
    }
}
