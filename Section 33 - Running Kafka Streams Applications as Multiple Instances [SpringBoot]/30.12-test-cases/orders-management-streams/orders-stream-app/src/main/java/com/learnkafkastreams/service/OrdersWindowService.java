package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.*;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;

@Service
@Slf4j
public class OrdersWindowService {

    private OrderStoreService orderStoreService;

    public OrdersWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {

        var countWindowsStore = getCountWindowsStore(orderType);

        var orderTypeEnum = mapOrderType(orderType);

        var countWindowsIterator = countWindowsStore.all();


        return mapToOrdersCountPerStoreByWindowsDTO(orderTypeEnum, countWindowsIterator);

    }

    private static List<OrdersCountPerStoreByWindowsDTO> mapToOrdersCountPerStoreByWindowsDTO(OrderType orderTypeEnum, KeyValueIterator<Windowed<String>, Long> countWindowsIterator) {
        var spliterator = Spliterators.spliteratorUnknownSize(countWindowsIterator, 0);

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(
                                keyValue.key.key(),
                                keyValue.value,
                                orderTypeEnum,
                                LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                                LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                )
                .collect(Collectors.toList());
    }

    private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows() {

        var generaLOrdersCountByWindows = getOrdersCountWindowsByType(GENERAL_ORDERS);
        var restaurantOrdersCountByWindows = getOrdersCountWindowsByType(RESTAURANT_ORDERS);

        return Stream.of(generaLOrdersCountByWindows, restaurantOrdersCountByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows(LocalDateTime fromTime, LocalDateTime toTime) {

        var fromTimeInstant = fromTime.toInstant(ZoneOffset.UTC);
        var toTimeInstant = toTime.toInstant(ZoneOffset.UTC);

        log.info("fromTimeInstant : {} , toTime : {} ", fromTimeInstant, toTimeInstant);

        var generalOrdersCountByWindows = getCountWindowsStore(GENERAL_ORDERS)
                //.fetchAll(fromTimeInstant, toTimeInstant)
                .backwardFetchAll(fromTimeInstant, toTimeInstant)
                ;


        var generalOrdersCountByWindowsDTO =
                mapToOrdersCountPerStoreByWindowsDTO(OrderType.GENERAL,generalOrdersCountByWindows);

        var restaurantOrdersCountByWindows = getCountWindowsStore(RESTAURANT_ORDERS)
                .backwardFetchAll(fromTimeInstant, toTimeInstant)
                ;

        var restaurantOrdersCountByWindowsDTO =
                mapToOrdersCountPerStoreByWindowsDTO(OrderType.RESTAURANT,restaurantOrdersCountByWindows);

        return Stream.of(generalOrdersCountByWindowsDTO, restaurantOrdersCountByWindowsDTO)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getOrdersRevenueWindowsByType(String orderType) {

        var revenueWindowsStore = getRevenueWindowsStore(orderType);

        var orderTypeEnum = mapOrderType(orderType);

        var revenueWindowsIterator = revenueWindowsStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(revenueWindowsIterator, 0);

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersRevenuePerStoreByWindowsDTO(
                                keyValue.key.key(),
                                keyValue.value,
                                orderTypeEnum,
                                LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                                LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                )
                .collect(Collectors.toList());
    }

    private ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowsStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
