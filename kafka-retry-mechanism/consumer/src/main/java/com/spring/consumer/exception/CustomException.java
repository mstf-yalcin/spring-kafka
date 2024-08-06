package com.spring.consumer.exception;

public class CustomException extends RuntimeException {
    public CustomException(String message) {
        super(message);
    }
}
