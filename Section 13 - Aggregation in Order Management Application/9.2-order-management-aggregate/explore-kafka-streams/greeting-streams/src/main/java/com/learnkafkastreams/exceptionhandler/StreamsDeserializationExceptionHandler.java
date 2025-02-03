package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

@Slf4j
public class StreamsDeserializationExceptionHandler implements DeserializationExceptionHandler {
    int errorCounter = 0;

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Exception is : {} , and the Kafka record is : {} ", exception.getMessage(),record,
                exception);
        log.info("errorCounter : {} ", errorCounter);
        if(errorCounter < 2){
            errorCounter++;
            return DeserializationHandlerResponse.CONTINUE;
        }
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
