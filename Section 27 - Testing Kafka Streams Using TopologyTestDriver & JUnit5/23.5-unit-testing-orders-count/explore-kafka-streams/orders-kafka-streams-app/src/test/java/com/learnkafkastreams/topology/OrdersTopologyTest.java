package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OrdersTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Order> ordersInputTopic = null;

    static String INPUT_TOPIC = ORDERS;

    @BeforeEach
    void setUp() {

        topologyTestDriver = new TopologyTestDriver(OrdersTopology.buildTopology());

        ordersInputTopic = topologyTestDriver.createInputTopic(ORDERS,
                Serdes.String().serializer(), SerdesFactory.orderSerdes().serializer()
        );


    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void ordersCount() {
        //given
        ordersInputTopic.pipeKeyValueList(orders());

        //when
       ReadOnlyKeyValueStore<String, Long> generalOrdersCountStore = topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_COUNT);

        var generalOrdersCount = generalOrdersCountStore.get("store_1234");
        assertEquals(1, generalOrdersCount);

        ReadOnlyKeyValueStore<String, Long> restaurantOrdersCountStore = topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_COUNT);

        var restaurantOrdersCount = restaurantOrdersCountStore.get("store_1234");
        assertEquals(1, restaurantOrdersCount);

    }

    static List<KeyValue<String, Order>> orders(){

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.now()
        );
        var keyValue1 = KeyValue.pair( order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair( order2.orderId().toString()
                , order2);


        return  List.of(keyValue1, keyValue2);

    }
}