package com.spring.producer.controller;


import com.spring.producer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api")
@Slf4j
public class TestController {


    private final KafkaTemplate<String, Object> kafkaTemplate;


    public TestController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/produce")
    public ResponseEntity<String> postMessage(@RequestBody User data) {


        Message<User> kafkaMessage = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.KEY, data.getId())
                .setHeader(KafkaHeaders.TOPIC, "test-topic.0")
//                .setHeader(KafkaHeaders.PARTITION, 0)
                .setHeader(KafkaHeaders.CORRELATION_ID, UUID.randomUUID().toString())
                .setHeader("custom-header", "header-value")
                .build();


        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(kafkaMessage);

        future.thenAcceptAsync(result -> {
            log.info("Sent message=[{}] offset=[{}] partition=[{}]",
                    kafkaMessage, result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
        }).exceptionally(ex -> {
            log.error("Unable to send message=[{}] due to : {}", kafkaMessage, ex.getMessage());
            return null;
        });

//        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("test-topic.0",data);
//        kafkaTemplate.send(producerRecord);
//
//        kafkaTemplate.send("test-topic.0", data);
        return ResponseEntity.ok("Message sent successfully" + data);
    }


}
