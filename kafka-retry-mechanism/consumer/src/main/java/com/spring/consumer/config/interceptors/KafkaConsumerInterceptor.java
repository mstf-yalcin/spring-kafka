package com.spring.consumer.config.interceptors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.Objects;
import java.util.UUID;

import static com.spring.consumer.config.Constants.*;
import static io.micrometer.common.util.StringUtils.isNotEmpty;

@Slf4j
public class KafkaConsumerInterceptor<T> implements RecordInterceptor<String, T> {
    @Override
    public ConsumerRecord<String, T> intercept(ConsumerRecord<String, T> record, Consumer<String, T> consumer) {
        setCorrelationId(record);
        return record;
    }

    private void setCorrelationId(ConsumerRecord<String, T> consumerRecord) {
        var correlationId = getHeaderStringValueByKey(consumerRecord, X_CORRELATION_ID);
        if (isNotEmpty(correlationId)) {
            MDC.put(X_CORRELATION_ID, correlationId);
            return;
        }
        MDC.put(X_CORRELATION_ID, UUID.randomUUID().toString());
    }


    private String getHeaderStringValueByKey(ConsumerRecord<String, T> consumerRecord, String headerKey) {
        for (Header header : consumerRecord.headers()) {
            if (Objects.equals(header.key(), headerKey)) {
                return new String(header.value());
            }
        }
        return "";
    }

}
