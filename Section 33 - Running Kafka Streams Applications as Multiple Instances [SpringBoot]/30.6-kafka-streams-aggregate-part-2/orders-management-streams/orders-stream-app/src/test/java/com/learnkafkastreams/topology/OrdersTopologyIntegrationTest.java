package com.learnkafkastreams.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.service.OrderService;
import com.learnkafkastreams.service.OrdersWindowService;
import org.apache.kafka.streams.KeyValue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {ORDERS, STORES})
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class OrdersTopologyIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OrderService orderService;

    @Autowired
    OrdersWindowService ordersWindowService;

    @BeforeEach
    public void setUp() {

        streamsBuilderFactoryBean.start();

    }

    @AfterEach
    public void destroy() {

        streamsBuilderFactoryBean.getKafkaStreams().close();
        streamsBuilderFactoryBean.getKafkaStreams().cleanUp();


    }

    @Test
    void ordersCount() {

        publishOrders();

        Awaitility.await().atMost(60, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCount(GENERAL_ORDERS, queryOtherHosts).size(), equalTo(1));

        var generalOrdersCount = orderService.getOrdersCount(GENERAL_ORDERS, queryOtherHosts);
        assertEquals(1, generalOrdersCount.get(0).orderCount());

    }

    @Test
    void ordersRevenue() {

        publishOrders();

        Awaitility.await().atMost(60, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderType(GENERAL_ORDERS).size(), equalTo(1));

        Awaitility.await().atMost(60, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderType(RESTAURANT_ORDERS).size(), equalTo(1));

        var generalOrdersRevenue = orderService.revenueByOrderType(GENERAL_ORDERS);
        assertEquals(new BigDecimal("27.00"), generalOrdersRevenue.get(0).totalRevenue().runningRevenue());

        var restaurantOrdersRevenue = orderService.revenueByOrderType(RESTAURANT_ORDERS);
        assertEquals(new BigDecimal("15.00"), restaurantOrdersRevenue.get(0).totalRevenue().runningRevenue());

    }

    @Test
    void ordersRevenue_multipleOrders() {

        publishOrders();
        publishOrders();

        Awaitility.await().atMost(60, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderType(GENERAL_ORDERS).size(), equalTo(1));

        Awaitility.await().atMost(60, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderType(RESTAURANT_ORDERS).size(), equalTo(1));

        var generalOrdersRevenue = orderService.revenueByOrderType(GENERAL_ORDERS);
        assertEquals(new BigDecimal("54.00"), generalOrdersRevenue.get(0).totalRevenue().runningRevenue());

        var restaurantOrdersRevenue = orderService.revenueByOrderType(RESTAURANT_ORDERS);
        assertEquals(new BigDecimal("30.00"), restaurantOrdersRevenue.get(0).totalRevenue().runningRevenue());

    }

    @Test
    void ordersRevenue_multipleOrdersByWindows() {

        publishOrders();
        publishOrders();

        Awaitility.await().atMost(60, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> ordersWindowService.getOrdersRevenueWindowsByType(GENERAL_ORDERS).size(), equalTo(1));

        Awaitility.await().atMost(60, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> ordersWindowService.getOrdersRevenueWindowsByType(RESTAURANT_ORDERS).size(), equalTo(1));

        var generalOrdersRevenue = ordersWindowService.getOrdersRevenueWindowsByType(GENERAL_ORDERS);
        assertEquals(new BigDecimal("54.00"), generalOrdersRevenue.get(0).totalRevenue().runningRevenue());
        System.out.println("generalOrdersRevenue : "+ generalOrdersRevenue);
        var expectedStartTime = LocalDateTime.parse("2023-02-22T03:25:00"); //"2023-02-21T21:25:01"
        var expectedEndTime = LocalDateTime.parse("2023-02-22T03:25:15"); //"2023-02-21T21:25:01"

        assert generalOrdersRevenue.get(0).startWindow().isEqual(expectedStartTime);
        assert generalOrdersRevenue.get(0).endWindow().isEqual(expectedEndTime);

        var restaurantOrdersRevenue = orderService.revenueByOrderType(RESTAURANT_ORDERS);
        assertEquals(new BigDecimal("30.00"), restaurantOrdersRevenue.get(0).totalRevenue().runningRevenue());

    }


    private void publishOrders() {
        orders()
                .forEach(order -> {
                    String orderJSON = null;
                    try {
                        orderJSON = objectMapper.writeValueAsString(order.value);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                        kafkaTemplate.send(ORDERS, order.key, orderJSON);
                });


    }


    static List<KeyValue<String, Order>> orders() {

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
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );
        var keyValue1 = KeyValue.pair(order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair(order2.orderId().toString()
                , order2);


        return List.of(keyValue1, keyValue2);

    }

    public static void deleteDirectory(File file)
    {
        for (File subfile : file.listFiles()) {
            System.out.println("Deleting : " + subfile.getName());
            if (subfile.isDirectory()) {
                deleteDirectory(subfile);
            }

            subfile.delete();
        }

        System.out.println("Delete Successful!");
    }


}
