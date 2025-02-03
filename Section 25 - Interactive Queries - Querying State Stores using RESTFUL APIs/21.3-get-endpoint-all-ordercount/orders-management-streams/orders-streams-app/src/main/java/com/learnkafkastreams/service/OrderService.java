package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrderService {

    private OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {

        var ordersCountStore = getOrderStore(orderType);
        var orders = ordersCountStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

    }

    private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Not a valid option");
        };

    }

    public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String locationId) {
        var ordersCountStore = getOrderStore(orderType);

        var orderCount = ordersCountStore.get(locationId);

        if (orderCount != null) {
            return new OrderCountPerStoreDTO(locationId, orderCount);
        }

        return null;

    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO>
                mapper = (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
                orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(), orderType);

        var generalOrdersCount = getOrdersCount(GENERAL_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                .toList();

        var restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
                .toList();

        return Stream.of(generalOrdersCount, restaurantOrdersCount)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }
}
