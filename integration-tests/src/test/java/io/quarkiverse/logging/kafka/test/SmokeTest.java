package io.quarkiverse.logging.kafka.test;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusTest
@QuarkusTestResource(KafkaResource.class)
public class SmokeTest {

    private static final String MSG = "Test!!";

    Logger log = LoggerFactory.getLogger(SmokeTest.class);

    @ConfigProperty(name = "quarkus.log.handler.kafka.broker-list")
    String brokerList;

    @Test
    public void testLogging() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Consumer<byte[], String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Set.of("mylog"));
        AtomicBoolean found = new AtomicBoolean();
        AtomicBoolean running = new AtomicBoolean(true);
        new Thread(() -> {
            while (running.get()) {
                ConsumerRecords<byte[], String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(cr -> {
                    if (cr.value().contains(MSG)) {
                        found.set(true);
                        running.set(false);
                    }
                });
                try {
                    //noinspection BusyWait
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();
        Thread.sleep(5000L); // wait 5sec
        log.warn(MSG);
        Thread.sleep(5000L); // wait 5sec
        running.set(false);
        consumer.close();
        Assertions.assertTrue(found.get());
    }
}
