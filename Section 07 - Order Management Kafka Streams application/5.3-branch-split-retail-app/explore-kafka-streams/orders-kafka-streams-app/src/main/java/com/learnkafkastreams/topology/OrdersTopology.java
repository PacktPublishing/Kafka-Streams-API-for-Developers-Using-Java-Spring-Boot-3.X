package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String STORES = "stores";


    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

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

                            generalOrdersStream
                                    .to(GENERAL_ORDERS,
                                            Produced.with(Serdes.String(), SerdesFactory.orderSerdes()));
                        })
                )
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            restaurantOrdersStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));

                            restaurantOrdersStream
                                    .to(RESTAURANT_ORDERS,
                                            Produced.with(Serdes.String(), SerdesFactory.orderSerdes()));

                        })
                );


        return streamsBuilder.build();

    }
}
