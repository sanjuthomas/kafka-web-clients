package com.sanjuthomas.kafkawebclients.support;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultSupportFactoriesTest {

    @Test
    void defaultKafkaReceiverFactoryCreatesReceiver() {
        DefaultKafkaReceiverFactory factory = new DefaultKafkaReceiverFactory();
        ReceiverOptions<String, String> options = ReceiverOptions.create(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));

        KafkaReceiver<String, String> receiver = factory.create(options);

        assertThat(receiver).isNotNull();
    }

    @Test
    void defaultKafkaSenderFactoryCreatesSender() {
        DefaultKafkaSenderFactory factory = new DefaultKafkaSenderFactory();
        SenderOptions<String, String> options = SenderOptions.create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

        KafkaSender<String, String> sender = factory.create(options);

        assertThat(sender).isNotNull();
        sender.close();
    }

    @Test
    void defaultAdminClientFactoryCreatesClient() {
        DefaultAdminClientFactory factory = new DefaultAdminClientFactory();

        try (AdminClient client = factory.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {
            assertThat(client).isNotNull();
        }
    }

    @Test
    void defaultAdminClientFacadeFactoryCreatesFacade() {
        DefaultAdminClientFacadeFactory factory = new DefaultAdminClientFacadeFactory(new DefaultAdminClientFactory());

        AdminClientFacade facade = factory.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));

        assertThat(facade).isInstanceOf(DefaultAdminClientFacade.class);
        facade.close();
    }
}
