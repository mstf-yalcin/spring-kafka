package com.spring.consumer.config;

public class Constants {
    public static final String X_CORRELATION_ID = "X-CorrelationId";
    public static final String X_RETRY_COUNT = "X-RetryCount";
    public static final String ERROR_SUFFIX = ".error";
    public static final String DLQ_SUFFIX = ".dlq";
    public static final int MAX_RETRY_COUNT = 3;
}
