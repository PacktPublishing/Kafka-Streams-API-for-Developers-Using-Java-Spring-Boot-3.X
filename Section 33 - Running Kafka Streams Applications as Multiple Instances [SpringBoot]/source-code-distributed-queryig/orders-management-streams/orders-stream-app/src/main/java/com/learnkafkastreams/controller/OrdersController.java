package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS;

@RestController
@RequestMapping("/v1/orders")
public class OrdersController {

    private OrderService orderService;

    public OrdersController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> ordersCount(
            @PathVariable("order_type") String orderType,
            @RequestParam(value="location_id", required=false) String locationId,
            @RequestParam(value="query_other_hosts", required=false) String queryOtherHosts
    ){
        if(!StringUtils.hasText(queryOtherHosts)){
            queryOtherHosts = "true";
        }

        if(StringUtils.hasLength(locationId)){
            return ResponseEntity.ok(orderService.getOrdersCountByLocationId(orderType, locationId));
        }

        return ResponseEntity.ok(orderService.getOrdersCount(orderType, queryOtherHosts));

    }

    @GetMapping("/count")
    public List<AllOrdersCountPerStoreDTO> allOrdersCount(){

        return orderService.getAllOrdersCount();

    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> revenueByOrderType(
            @PathVariable("order_type") String orderType
            //@RequestParam(value="location_id", required=false) String locationId
    ){

//        if(StringUtils.hasLength(locationId)){
//            return ResponseEntity.ok(orderService.getOrdersCountByLocationId(orderType, locationId));
//        }

        return ResponseEntity.ok(orderService.revenueByOrderType(orderType));

    }




}
