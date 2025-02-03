package com.learnkafkastreams.util;

import com.learnkafkastreams.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var order = (Order) record.value();
        if(order!=null && order.orderedDateTime()!=null){
            var timeStamp = order.orderedDateTime();
            log.info("timeStamp in extractor : {} ", timeStamp);

            return convertToInstantFromCST(timeStamp);
        }

        return partitionTime;
    }

    private long convertToInstantFromCST(LocalDateTime timeStamp) {

        return timeStamp.toInstant(ZoneOffset.ofHours(-6)).toEpochMilli();

    }

    private long convertToInstantFromUTC(LocalDateTime timeStamp) {

        return timeStamp.toInstant(ZoneOffset.UTC).toEpochMilli();

    }
}
