package com.learnkafkastreams.service;

import com.learnkafkastreams.client.OrdersServiceClient;
import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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

    private OrdersServiceClient ordersServiceClient;

    private MetaDataService metaDataService;

    @Value("${server.port}")
    private Integer port;

    public OrderService(OrderStoreService orderStoreService, OrdersServiceClient ordersServiceClient, MetaDataService metaDataService) {
        this.orderStoreService = orderStoreService;
        this.ordersServiceClient = ordersServiceClient;
        this.metaDataService = metaDataService;
    }


    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType, String queryOtherHosts) {

        var ordersCountStore = getOrderStore(orderType);
        var orders = ordersCountStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);
        var orderCountPerStoreDTOListCurrentInstance = StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

        //1. fetch the metadata about other instances

        //2. make the restcall to get the data from other instance
            // make sure the other instance is not going to make any network calls to other instances
        var orderCountPerStoreDTOList = retrieveDataFromOtherInstances(orderType,
                Boolean.parseBoolean(queryOtherHosts));

        log.info("orderCountPerStoreDTOList :  {} , orderCountPerStoreDTOList : {} " ,orderCountPerStoreDTOListCurrentInstance, orderCountPerStoreDTOList );
        //3. aggregate the data.

        return Stream.of(orderCountPerStoreDTOListCurrentInstance,
                orderCountPerStoreDTOList)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    private List<OrderCountPerStoreDTO> retrieveDataFromOtherInstances(String orderType, boolean queryOtherHosts)  {
        var otherHosts = otherHosts();
        log.info("otherHosts : {} ", otherHosts);

        if (queryOtherHosts && otherHosts != null && !otherHosts.isEmpty()) {
            //2. make the restcall to get the data from other instance
             // make sure the other instance is not going to make any network calls to other instances

            return otherHosts()
                    .stream()
                    .map(hostInfoDTO -> ordersServiceClient.retrieveOrdersCountByOrderType(
                            hostInfoDTO, orderType
                    ))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        return null;
    }

    private List<HostInfoDTO> otherHosts() {

        try {
            var currentMachineAddress = InetAddress.getLocalHost().getHostAddress();
            return metaDataService.getStreamsMetaData()
                    .stream()
                    .filter(hostInfoDTO -> currentMachineAddress.equals(hostInfoDTO.host()) && hostInfoDTO.port() != port)
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
        var storeName = mapOrderCountStoreName(orderType);
        var hostMetaData  = metaDataService.getStreamsMetaData(storeName, locationId);
        log.info("hostMetaData : {} ", hostMetaData);
        if(hostMetaData!=null){
            if(hostMetaData.port()==port){
                log.info("Fetching the data from the current instance");
                var ordersCountStore = getOrderStore(orderType);
                var orderCount = ordersCountStore.get(locationId);
                if (orderCount != null) {
                    return new OrderCountPerStoreDTO(locationId, orderCount);
                }
                return null;
            }else{
                log.info("Fetching the data from the remote instance");
                // remote instance
                // build a rest client
                var orderCountPerStoreDTO = ordersServiceClient.retrieveOrdersCountByOrderTypeAndLocationId(
                        new HostInfoDTO(hostMetaData.host(), hostMetaData.port()),
                        orderType,locationId
                );

                return orderCountPerStoreDTO;
            }

        }
        return null;

    }

    private String mapOrderCountStoreName(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> GENERAL_ORDERS_COUNT;
            case RESTAURANT_ORDERS -> RESTAURANT_ORDERS_COUNT;
            default -> throw new IllegalStateException("Not a valid option");
        };


    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO>
                mapper = (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
                orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(), orderType);

        var generalOrdersCount = getOrdersCount(GENERAL_ORDERS, "true")
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                .toList();

        var restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS, "true")
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
