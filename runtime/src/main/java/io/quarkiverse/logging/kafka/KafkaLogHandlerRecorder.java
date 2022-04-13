package io.quarkiverse.logging.kafka;

import java.util.Optional;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.kafka.log4jappender.KafkaLog4jAppender;
import org.apache.log4j.Layout;
import org.jboss.logmanager.ExtHandler;
import org.jboss.logmanager.handlers.AsyncHandler;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

/**
 * The recorder providing configured {@link KafkaLog4jAppender} wrapped in a {@link Log4jAppenderHandler}. Optionally
 * the result can be wrapped in an {@link AsyncHandler} instance. The format of the produced log can be defined by
 * a {@link Formatter} or a {@link Layout}. That can be injected by implementation of {@link DefaultFormatterOrLayoutProducer}.
 * If no implementation of that interface is available, it uses {@link PncLoggingLayout}.
 *
 * @author <a href="mailto:pkocandr@redhat.com">Petr Kocandrle</a>
 */
@Recorder
public class KafkaLogHandlerRecorder {

    private static final Logger loggingLogger = Logger.getLogger("io.quarkiverse.logging.kafka.KafkaLogHandlerRecorder");

    @Inject
    FormatterOrLayout formatterOrLayout;

    /**
     * Kafka logging handler initialization based on the config.
     *
     * @param config the kafka log config
     * @return initialized handler if configured
     */
    public RuntimeValue<Optional<Handler>> initializeHandler(final KafkaLogConfig config) {
        if (!config.enabled) {
            return new RuntimeValue<>(Optional.empty());
        }

        KafkaLog4jAppender appender = createAppender(config);

        Log4jAppenderHandler kafkaHandler = new Log4jAppenderHandler(appender, false);

        kafkaHandler.setLevel(config.level);

        // set a formatter or a layout
        if (formatterOrLayout == null) {
            loggingLogger.warning("No formatter or layout for Kafka logger provided.");
            String timestampPattern = null;
            if (config.timestampPattern.isPresent()) {
                timestampPattern = config.timestampPattern.get();
            }
            formatterOrLayout = DefaultFormatterOrLayoutProducer.pncLayout(timestampPattern);
        }

        if (formatterOrLayout.hasLayout()) {
            appender.setLayout(formatterOrLayout.getLayout());
            loggingLogger.config("Configured with layout: " + appender.getLayout());
        } else if (formatterOrLayout.hasFormatter()) {
            kafkaHandler.setFormatter(formatterOrLayout.getFormatter());
            loggingLogger.config("Configured with formatter: " + kafkaHandler.getFormatter());
        } else {
            loggingLogger.warning(
                    "No formatter nor layout for kafka logger was present in the FormatterOrLayout instance: "
                            + formatterOrLayout);
        }

        ExtHandler rootHandler;

        if (config.async) {
            AsyncHandler asyncWrapper = config.asyncQueueLength.map(AsyncHandler::new).orElseGet(AsyncHandler::new);
            config.asyncOverflowAction.ifPresent(asyncWrapper::setOverflowAction);
            asyncWrapper.setLevel(config.level);

            asyncWrapper.addHandler(kafkaHandler);

            rootHandler = asyncWrapper;
        } else {
            rootHandler = kafkaHandler;
        }

        return new RuntimeValue<>(Optional.of(rootHandler));
    }

    private KafkaLog4jAppender createAppender(final KafkaLogConfig config) {
        loggingLogger.config("Processing config to create KafkaLog4jAppender: " + config);

        KafkaLog4jAppender appender = new KafkaLog4jAppender();
        appender.setBrokerList(config.brokerList);
        appender.setTopic(config.topic);

        config.compressionType.ifPresent(appender::setCompressionType);
        config.securityProtocol.ifPresent(appender::setSecurityProtocol);
        config.sslTruststoreLocation.ifPresent(appender::setSslTruststoreLocation);
        config.sslTruststorePassword.ifPresent(appender::setSslTruststorePassword);
        config.sslKeystoreType.ifPresent(appender::setSslKeystoreType);
        config.sslKeystoreLocation.ifPresent(appender::setSslKeystoreLocation);
        config.sslKeystorePassword.ifPresent(appender::setSslKeystorePassword);
        config.saslKerberosServiceName.ifPresent(appender::setSaslKerberosServiceName);
        config.clientJaasConfPath.ifPresent(appender::setClientJaasConfPath);
        config.kerb5ConfPath.ifPresent(appender::setKerb5ConfPath);
        config.maxBlockMs.ifPresent(appender::setMaxBlockMs);

        config.retries.ifPresent(appender::setRetries);
        config.requiredNumAcks.ifPresent(appender::setRequiredNumAcks);
        config.deliveryTimeoutMs.ifPresent(appender::setDeliveryTimeoutMs);
        config.ignoreExceptions.ifPresent(appender::setIgnoreExceptions);
        config.syncSend.ifPresent(appender::setSyncSend);

        loggingLogger.finer("Running appender.activateOptions()");
        appender.activateOptions();
        loggingLogger.finer("Finished appender.activateOptions()");

        return appender;
    }

}
