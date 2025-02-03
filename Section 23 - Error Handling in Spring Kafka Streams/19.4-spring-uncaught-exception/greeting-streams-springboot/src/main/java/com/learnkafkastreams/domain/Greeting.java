package com.learnkafkastreams.domain;

import java.time.LocalDateTime;

public record Greeting(String message,
                       LocalDateTime timeStamp
) {
}
