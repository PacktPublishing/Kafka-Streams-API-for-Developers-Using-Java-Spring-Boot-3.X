package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

public class ExploreKTableTopology {

    public static String WORDS = "words";


    public static Topology build(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var wordsTable = streamsBuilder
                .table(WORDS,
                        Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("words-store")
                        );

        wordsTable
                .filter((key, value) -> value.length() > 2)
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("words-ktable"));


        return streamsBuilder.build();

    }
}
