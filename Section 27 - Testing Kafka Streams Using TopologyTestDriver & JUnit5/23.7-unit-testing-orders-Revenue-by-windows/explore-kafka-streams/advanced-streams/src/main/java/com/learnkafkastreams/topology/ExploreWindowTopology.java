package com.learnkafkastreams.topology;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.*;
import java.time.format.DateTimeFormatter;

@Slf4j
public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var wordsStream = streamsBuilder
                .stream(WINDOW_WORDS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        //tumblingWindows(wordsStream);

        //hoppingWindows(wordsStream);
        slidingWindows(wordsStream);

        return streamsBuilder.build();
    }

    private static void tumblingWindows(KStream<String, String> wordsStream) {

        var windowSize = Duration.ofSeconds(5);
        var timeWindow = TimeWindows.ofSizeWithNoGrace(windowSize);

        var windowedAggregation = wordsStream
                .groupByKey()
                .windowedBy(timeWindow)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        windowedAggregation
                .toStream()
                .peek((key, value) -> {
                    log.info("tumblingWindow : key : {} , value : {} ", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumblingWindow"));

    }

    private static void hoppingWindows(KStream<String, String> wordsStream) {

        var windowSize = Duration.ofSeconds(5);

        Duration.ofDays(1);

        var advancedBySize = Duration.ofSeconds(3);
        var timeWindow = TimeWindows.ofSizeWithNoGrace(windowSize)
                .advanceBy(advancedBySize);

        var windowedAggregation = wordsStream
                .groupByKey()
                .windowedBy(timeWindow)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        windowedAggregation
                .toStream()
                .peek((key, value) -> {
                    log.info("hoppingWindows : key : {} , value : {} ", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumblingWindow"));

    }

    private static void slidingWindows(KStream<String, String> wordsStream) {

        var windowSize = Duration.ofSeconds(5);

        var slidingWindow = SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

        var windowedAggregation = wordsStream
                .groupByKey()
                .windowedBy(slidingWindow)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        windowedAggregation
                .toStream()
                .peek((key, value) -> {
                    log.info("slidingWindows : key : {} , value : {} ", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumblingWindow"));

    }


    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime : {} , endTime : {}, Count : {}", startTime, endTime, value);

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

}
