package io.quarkiverse.logging.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/**
 * A logging layout fit for PNC Quarkus components. Based on {@code net.logstash.log4j.JSONEventLayoutV1}.
 *
 * @author pkocandr
 */
public class PncLoggingLayout extends Layout {

    private final FastDateFormat timestampFormat;

    private boolean ignoreThrowable = false;

    private String hostname;
    private static Integer version = 1;

    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final ObjectMapper MAPPER = new JsonMapper();

    /**
     * Creates a layout that optionally inserts location information into log messages.
     *
     * @param timestampFormat timestamp format
     */
    public PncLoggingLayout(String timestampFormat) {
        String format = Objects.requireNonNullElse(timestampFormat, DEFAULT_DATETIME_FORMAT);
        try {
            this.hostname = java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "unknown-host";
        }
        this.timestampFormat = FastDateFormat.getInstance(format, UTC);
    }

    private String dateFormat(long timestamp) {
        return timestampFormat.format(timestamp);
    }

    @Override
    public String format(LoggingEvent loggingEvent) {
        String threadName = loggingEvent.getThreadName();
        long timestamp = loggingEvent.getTimeStamp();
        HashMap<String, Object> exceptionInformation = new HashMap<>();
        @SuppressWarnings("rawtypes")
        Map mdc = loggingEvent.getProperties();
        String ndc = loggingEvent.getNDC();

        ObjectNode jsonEvent = MAPPER.createObjectNode();

        jsonEvent.put("@version", version);
        jsonEvent.put("@timestamp", dateFormat(timestamp));

        jsonEvent.put("hostName", hostname);
        jsonEvent.put("message", loggingEvent.getRenderedMessage());

        if (loggingEvent.getThrowableInformation() != null) {
            final ThrowableInformation throwableInformation = loggingEvent.getThrowableInformation();
            if (throwableInformation.getThrowable().getClass().getCanonicalName() != null) {
                exceptionInformation
                        .put("exceptionClass", throwableInformation.getThrowable().getClass().getCanonicalName());
            }
            if (throwableInformation.getThrowable().getMessage() != null) {
                exceptionInformation.put("exceptionMessage", throwableInformation.getThrowable().getMessage());
            }
            if (throwableInformation.getThrowableStrRep() != null) {
                String stackTrace = String.join("\n", Arrays.asList(throwableInformation.getThrowableStrRep()));
                exceptionInformation.put("stacktrace", stackTrace);
            }
            addEventData(jsonEvent, "exception", exceptionInformation);
        }

        // location info does not work with Quarkus for some reason
        // LocationInfo info = loggingEvent.getLocationInformation();
        // addEventData(jsonEvent, "file", info.getFileName());
        // addEventData(jsonEvent, "lineNumber", info.getLineNumber());
        // addEventData(jsonEvent, "class", info.getClassName());
        // addEventData(jsonEvent, "method", info.getMethodName());

        addEventData(jsonEvent, "loggerName", loggingEvent.getLoggerName());
        addEventData(jsonEvent, "mdc", mdc);
        addEventData(jsonEvent, "ndc", ndc);
        addEventData(jsonEvent, "level", loggingEvent.getLevel());
        addEventData(jsonEvent, "threadName", threadName);

        return jsonEvent + "\n";
    }

    @Override
    public boolean ignoresThrowable() {
        return ignoreThrowable;
    }

    @Override
    public void activateOptions() {
    }

    private void addEventData(ObjectNode jsonEvent, String keyname, Object keyval) {
        if (keyval != null) {
            jsonEvent.put(keyname, keyval.toString());
        }
    }

    @Override
    public String toString() {
        return "PncLoggingLayout [timestampFormat=" + timestampFormat + ", ignoreThrowable=" + ignoreThrowable
                + ", hostname=" + hostname + "]";
    }

}
