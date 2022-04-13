package io.quarkiverse.logging.kafka.app;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.quarkiverse.logging.kafka.DefaultFormatterOrLayoutProducer;
import io.quarkiverse.logging.kafka.FormatterOrLayout;

@ApplicationScoped
public class Configuration {

    @ApplicationScoped
    @Produces
    public FormatterOrLayout customFormat() {
        return DefaultFormatterOrLayoutProducer.jsonFormatter("HH:mm:ss DD/MM/YYYY");
    }
}
