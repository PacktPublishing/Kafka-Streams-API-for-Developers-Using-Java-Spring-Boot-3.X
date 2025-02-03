package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";

    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";

    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String STORES = "stores";


    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var ordersStream = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), SerdesFactory.orderSerdes())
                );

        ordersStream
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));

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
                            aggregateOrdersByCount(generalOrdersStream, GENERAL_ORDERS_COUNT);
                           // aggregateOrdersByRevenue(generalOrdersStream, GENERAL_ORDERS_REVENUE);

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

                            aggregateOrdersByCount(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT);
                            //aggregateOrdersByRevenue(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE);
                        })
                );


        return streamsBuilder.build();

    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrdersStream, String storeName) {

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


    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName) {

        var ordersCountPerStore = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));


    }
}
