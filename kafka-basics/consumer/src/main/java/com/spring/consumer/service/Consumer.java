package com.spring.consumer.service;


import com.spring.consumer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class Consumer {

    @KafkaListener(topics = "test-topic.0", groupId = "${spring.application.name}-group-1")
    public void consume(@Payload User message,
                        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        @Header("custom-header") String customHeader, Acknowledgment acknowledgment) {

        log.info("Received message consumer-group-1: {}", message);
        log.info("Received message key consumer-group-1: {} ", key);
        log.info("Received topic: {} consumer-group-1:", topic);
        log.info("Received partition consumer-group-1: {}", partition);
        log.info("Received offset consumer-group-1: {}", offset);
        log.info("Custom header consumer-group-1: {}", customHeader);

        acknowledgment.acknowledge();
    }


    @KafkaListener(topics = "test-topic.0", groupId = "${spring.application.name}-group-2")
    public void consume(@Payload User message, ConsumerRecord<String, Object> record) {

        log.info("Received message consumer-group-2: {}", message);
        log.info("Received message key consumer-group-2:: {}", record.key());
        log.info("Received topic consumer-group-2:: {}", record.topic());
        log.info("Received partition consumer-group-2:: {}", record.partition());
        log.info("Received offset consumer-group-2:: {}", record.offset());

        Headers headers = record.headers();
        headers.forEach(header -> {
            log.info("consumer-group-2 Header key: {}, value: {}", header.key(), new String(header.value()));
        });

    }


}
