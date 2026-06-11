package com.sanjuthomas.kafkawebclients.support;

import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConfigSupportTest {

    private final KafkaConfigSupport support = new KafkaConfigSupport();

    @Test
    void buildConsumerPropertiesIncludesOffsetResetAndAdditionalProperties() {
        StreamConfig config = new StreamConfig(
                " localhost:9092 ",
                "events",
                "# comment\nsecurity.protocol=SASL_SSL\ninvalid-line\nauto.offset.reset=earliest",
                "earliest",
                null);

        var props = support.buildConsumerProperties(config);

        assertThat(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(props.get("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(props.get(ConsumerConfig.GROUP_ID_CONFIG)).asString().startsWith("kafka-web-clients-");
    }

    @Test
    void buildProducerPropertiesIncludesSerializersAndClientId() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);

        var props = support.buildProducerProperties(config);

        assertThat(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        assertThat(props.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(props.get(ProducerConfig.CLIENT_ID_CONFIG)).asString().startsWith("kafka-web-clients-producer-");
    }

    @Test
    void buildAdminPropertiesIncludesTimeouts() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", null, null);

        var props = support.buildAdminProperties(config);

        assertThat(props.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo(10_000);
        assertThat(props.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)).isEqualTo(10_000);
        assertThat(props.get(AdminClientConfig.CLIENT_ID_CONFIG)).asString().startsWith("kafka-web-clients-connectivity-");
    }

    @Test
    void resolveAutoOffsetResetDefaultsToLatest() {
        StreamConfig blank = new StreamConfig("localhost:9092", "events", "", "  ", null);
        StreamConfig invalid = new StreamConfig("localhost:9092", "events", "", "newest", null);

        assertThat(support.resolveAutoOffsetReset(nullConfig())).isEqualTo("latest");
        assertThat(support.resolveAutoOffsetReset(blank)).isEqualTo("latest");
        assertThat(support.resolveAutoOffsetReset(invalid)).isEqualTo("latest");
    }

    @Test
    void resolveAutoOffsetResetAcceptsEarliest() {
        StreamConfig config = new StreamConfig("localhost:9092", "events", "", " EARLIEST ", null);

        assertThat(support.resolveAutoOffsetReset(config)).isEqualTo("earliest");
    }

    @Test
    void resolveConsumerGroupUsesProvidedNameOrRandomId() {
        StreamConfig named = new StreamConfig("localhost:9092", "events", "", null, " my-group ");
        StreamConfig anonymous = new StreamConfig("localhost:9092", "events", "", null, null);

        assertThat(support.resolveConsumerGroup(named)).isEqualTo("my-group");
        assertThat(support.resolveConsumerGroup(anonymous)).startsWith("kafka-web-clients-");
        assertThat(support.hasNamedConsumerGroup(named)).isTrue();
        assertThat(support.hasNamedConsumerGroup(anonymous)).isFalse();
    }

    @Test
    void resolveOffsetResetAcceptsCommittedAndMapsKafkaFallback() {
        StreamConfig committed = new StreamConfig("localhost:9092", "events", "", "committed", "my-group");

        assertThat(support.resolveOffsetReset(committed)).isEqualTo("committed");
        assertThat(support.resolveAutoOffsetReset(committed)).isEqualTo("earliest");
    }

    private StreamConfig nullConfig() {
        return new StreamConfig("localhost:9092", "events", "", null, null);
    }
}
