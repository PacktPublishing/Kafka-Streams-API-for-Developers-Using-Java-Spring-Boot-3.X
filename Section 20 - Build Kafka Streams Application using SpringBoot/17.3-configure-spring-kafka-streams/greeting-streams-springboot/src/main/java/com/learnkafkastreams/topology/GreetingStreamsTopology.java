package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GreetingStreamsTopology {

    public static String GREETINGS="greetings";
    public static String GREETINGS_OUTPUT="greetings-output";

    @Autowired
    public void process(StreamsBuilder streamsBuilder){

        var greetingsStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        greetingsStream
                .print(Printed.<String ,String>toSysOut().withLabel("greetingsStream"));

        var modifiedStream = greetingsStream
                .mapValues((readOnlyKey, value) -> value.toUpperCase());


        modifiedStream
                .print(Printed.<String ,String>toSysOut().withLabel("modifiedStream"));

        modifiedStream
                .to(GREETINGS_OUTPUT,
                        Produced.with(Serdes.String(), Serdes.String()));

    }
}
