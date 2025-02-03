package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetingsStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        var greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH,
                        Consumed.with(Serdes.String(), Serdes.String()));

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);

        mergedStream
                .print(Printed.<String, String>toSysOut().withLabel("mergedStream"));

        var modifiedStream = mergedStream
//                .filter((key, value) -> value.length() > 5)
//                .peek((key, value) -> {
//                    log.info("after filter : key : {}, value : {} ",key, value );
//                })
                .mapValues((readOnlyKey, value) -> value.toUpperCase())
//                .peek((key, value) -> {
//                    key.toUpperCase();
//                    log.info("after mapValues : key : {}, value : {} ",key, value );
//                })
                //.map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))
//                .flatMap((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
//                    return newValues
//                            .stream()
//                            .map(val -> KeyValue.pair(key, val.toUpperCase()))
//                            .collect(Collectors.toList());
//                })
//                .flatMapValues((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
//                    return newValues
//                            .stream()
//                            .map(String::toUpperCase)
//                            .collect(Collectors.toList());
//                })
                ;

        modifiedStream
                .print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));


        modifiedStream
                .to(GREETINGS_UPPERCASE,
                        Produced.with(Serdes.String(), Serdes.String()));


        return streamsBuilder.build();

    }
}
