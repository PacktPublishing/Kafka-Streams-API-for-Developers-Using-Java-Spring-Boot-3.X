package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.producer.MetaDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

    private MetaDataService metaDataService;

    @Value("${server.port}")
    private Integer port;

    public OrderService(OrderStoreService orderStoreService, MetaDataService metaDataService) {
        this.orderStoreService = orderStoreService;
        this.metaDataService = metaDataService;
    }


    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {

        var ordersCountStore = getOrderStore(orderType);
        var orders = ordersCountStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

        //1. fetch the metadata about other instances

        retrieveDataFromOtherInstances(orderType);

        //2. make the restcall to get the data from other instance
            // make sure the other instance is not going to make any network calls to other instances

        //3. aggregate the data.

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

    }

    private void retrieveDataFromOtherInstances(String orderType)  {
        var otherHosts = otherHosts();
        log.info("otherHosts : {} ", otherHosts);

        if (otherHosts != null && !otherHosts.isEmpty()) {


        }

    }

    private List<HostInfoDTO> otherHosts() {

        try {
            var currentMachineAddress = InetAddress.getLocalHost().getHostAddress();
            return metaDataService.getStreamsMetaData()
                    .stream()
                    .filter(hostInfoDTO -> !currentMachineAddress.equals(hostInfoDTO.host()) && hostInfoDTO.port() != port)
                    .collect(Collectors.toList());
        } catch (UnknownHostException e) {
            log.error("Exception in otherHosts : {} ", e.getMessage(), e);
        }
        return null;
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

    public List<OrderRevenueDTO> revenueByOrderType(String orderType) {

        var revenueStoreByType = getRevenueStore(orderType);

        var revenueIterator = revenueStoreByType.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenueIterator, 0);

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
                .collect(Collectors.toList());

    }

    public static OrderType mapOrderType(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersRevenueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.ordersRevenueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
