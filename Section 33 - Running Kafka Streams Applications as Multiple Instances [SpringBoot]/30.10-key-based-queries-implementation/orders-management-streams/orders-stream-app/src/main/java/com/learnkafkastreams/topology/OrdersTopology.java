package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
@Slf4j
public class OrdersTopology {

    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";

    public static final String STORES = "stores";



    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        orderTopology(streamsBuilder);

    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {
        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Store.class)));

        storesTable
                .toStream()
                .print(Printed.<String,Store>toSysOut().withLabel("stores"));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));


        orderStreams
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {

                            generalOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("generalStream"));

                            aggregateOrdersByCount(generalOrdersStream, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(generalOrdersStream, GENERAL_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(generalOrdersStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersRevenueByTimeWindows(generalOrdersStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);

                        })
                )
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            restaurantOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));


                            aggregateOrdersByCount(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE,storesTable);
                            aggregateOrdersRevenueByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);
                        })
                );



    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        var ordersCountPerStore = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        var revenueWithStoreTable = ordersCountPerStore
                .join(storesTable, valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName+"-bystore"));


    }

    private static void aggregateOrdersCountByTimeWindows
            (KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(60);
        Duration graceWindowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(windowSize, graceWindowSize);

        var ordersCountPerStore = generalOrdersStream
                //.map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .windowedBy(timeWindows)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        ordersCountPerStore
                .toStream()
                //.print(Printed.<String, Long>toSysOut().withLabel(storeName));
                .peek((key, value) -> {
                    log.info(" StoreName : {}, key: {} , value : {}  ", storeName, key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

//        var revenueWithStoreTable = ordersCountPerStore
//                .join(storesTable, valueJoiner);
//
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName+"-bystore"));
    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        Initializer<TotalRevenue> totalRevenueInitializer
                =TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator
                =(key, value, aggregate) -> aggregate.updateRunningRevenue(key,value);

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<TotalRevenue>(TotalRevenue.class))
                );

        revenueTable
                .toStream()
                .print(Printed.<String,TotalRevenue>toSysOut().withLabel(storeName));

        //KTable-KTable join

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var revenueWithStoreTable = revenueTable
                .join(storesTable, valueJoiner);


        revenueWithStoreTable
                .toStream()
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));


    }


    private static void aggregateOrdersRevenueByTimeWindows(KStream<String, Order> generalOrdersStream, String storeName,
                                                            KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        Initializer<TotalRevenue> totalRevenueInitializer
                =TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator
                =(key, value, aggregate) -> aggregate.updateRunningRevenue(key,value);

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .windowedBy(timeWindows)
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<TotalRevenue>(TotalRevenue.class))
                );

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info(" StoreName : {}, key: {} , value : {}  ", storeName, key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>,TotalRevenue>toSysOut().withLabel(storeName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var joinedParams =
                Joined.with(Serdes.String(),new JsonSerde<TotalRevenue>(TotalRevenue.class), new JsonSerde<Store>(Store.class));

        revenueTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storesTable, valueJoiner, joinedParams)
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));


//        revenueWithStoreTable
//                .toStream()
//                ;

    }

    private static void printLocalDateTimes(Windowed<String> key, Object value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime : {} , endTime : {}, Count : {}", startTime, endTime, value);
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

}
