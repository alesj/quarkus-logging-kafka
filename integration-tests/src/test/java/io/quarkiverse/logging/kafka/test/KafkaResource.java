package io.quarkiverse.logging.kafka.test;

import java.util.Map;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.strimzi.test.container.StrimziKafkaContainer;

public class KafkaResource implements QuarkusTestResourceLifecycleManager {

    StrimziKafkaContainer container = new StrimziKafkaContainer();

    @Override
    public Map<String, String> start() {
        container.start();
        return Map.of("quarkus.log.handler.kafka.broker-list", container.getBootstrapServers());
    }

    @Override
    public void stop() {
        container.stop();
    }
}
