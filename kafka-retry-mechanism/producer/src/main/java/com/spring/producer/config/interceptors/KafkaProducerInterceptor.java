package com.spring.producer.config.interceptors;

import io.micrometer.common.util.StringUtils;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static com.spring.producer.config.Constants.X_CORRELATION_ID;

public class KafkaProducerInterceptor implements ProducerInterceptor<String, Object> {
    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        setCorrelationId(record);
        return record;
    }

    private void setCorrelationId(ProducerRecord<String, Object> record) {
        String correlationId = MDC.get(X_CORRELATION_ID);
        if (StringUtils.isBlank(correlationId)) {
            correlationId = UUID.randomUUID().toString();
        }

        record.headers().add(X_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
    }


    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
