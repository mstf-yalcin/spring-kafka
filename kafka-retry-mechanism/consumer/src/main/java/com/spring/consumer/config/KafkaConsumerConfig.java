package com.spring.consumer.config;

import com.spring.consumer.config.interceptors.KafkaConsumerInterceptor;
import com.spring.consumer.exception.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.spring.consumer.config.Constants.*;

@Configuration
@Slf4j
public class KafkaConsumerConfig {
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "${spring.application.name}-group-1");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
//        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
//        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
//        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.spring.consumer.model.User");
//        config.put(JsonDeserializer.TYPE_MAPPINGS, "com.spring.producer.model.User:com.spring.consumer.model.User");

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(CommonErrorHandler commonErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setRecordMessageConverter(new StringJsonMessageConverter());
        factory.setRecordFilterStrategy(consumerRecord -> consumerRecord.value().toString().contains("ignore"));
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(commonErrorHandler);
        factory.setRecordInterceptor(new KafkaConsumerInterceptor());
        return factory;
    }

    @Bean
    public CommonErrorHandler commonErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
                    log.error("Error while processing event: {}", record, ex);

                    int retryCount = getRetryCountFromHeader(record);
                    record.headers().remove(X_RETRY_COUNT);
                    record.headers().add(new RecordHeader(X_RETRY_COUNT, String.valueOf(retryCount + 1).getBytes()));

                    var topicName = record.topic().replace(ERROR_SUFFIX, "") + ERROR_SUFFIX;

                    if (retryCount >= MAX_RETRY_COUNT || ex.getCause() instanceof CustomException) {
                        topicName = record.topic().replace(ERROR_SUFFIX, "") + DLQ_SUFFIX;
                    }

                    return new TopicPartition(topicName, -1);
                }),//2
                new FixedBackOff(100, 3) //1
        );
    }

    private int getRetryCountFromHeader(ConsumerRecord<?, ?> consumerRecord) {
        for (Header header : consumerRecord.headers()) {
            if (Objects.equals(header.key(), X_RETRY_COUNT)) {
                return Integer.parseInt(new String(header.value()));
            }
        }
        return 0;
    }


}


