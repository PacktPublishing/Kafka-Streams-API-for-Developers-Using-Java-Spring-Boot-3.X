package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";

    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";

    public static final String RESTAURANT_ORDERS_REVENUE_WINDOW = "restaurant_orders_revenue_window";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";

    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";
    public static final String STORES = "stores";


    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var ordersStream = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), SerdesFactory.orderSerdes())
                )
                .selectKey((key, value) -> value.locationId());

        ordersStream
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));

        //KStream-KTable

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), SerdesFactory.storeSerdes()));

        ordersStream
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {

                            generalOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("generalStream"));

//                            generalOrdersStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(GENERAL_ORDERS,
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                            //aggregateOrdersByCount(generalOrdersStream, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(generalOrdersStream, GENERAL_ORDERS_COUNT_WINDOWS, storesTable);
                            //aggregateOrdersByRevenue(generalOrdersStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersRevenueByTimeWindows(generalOrdersStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);

                        })
                )
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            restaurantOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));

//                            restaurantOrdersStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS,
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));

                            //aggregateOrdersByCount(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storesTable);
                            //aggregateOrdersByRevenue(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE,storesTable);
                            aggregateOrdersRevenueByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE_WINDOW, storesTable);
                        })
                );


        return streamsBuilder.build();

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
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
                );

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info(" StoreName : {}, key: {} , value : {}  ", storeName, key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>,TotalRevenue>toSysOut().withLabel(storeName));

        //KTable-KTable join

//        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
//
//        var revenueWithStoreTable = revenueTable
//                .join(storesTable, valueJoiner);
//
//
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));

    }

    private static void aggregateOrdersCountByTimeWindows
            (KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        var ordersCountPerStore = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
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
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
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

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        var ordersCountPerStore = generalOrdersStream
                //.map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
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

    private static void printLocalDateTimes(Windowed<String> key, Object value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime : {} , endTime : {}, Count : {}", startTime, endTime, value);
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }
}
