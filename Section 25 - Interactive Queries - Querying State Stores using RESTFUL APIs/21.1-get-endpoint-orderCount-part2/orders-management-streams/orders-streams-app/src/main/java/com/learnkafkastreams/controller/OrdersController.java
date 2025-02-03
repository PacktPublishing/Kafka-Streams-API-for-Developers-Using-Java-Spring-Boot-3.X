package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
    public List<OrderCountPerStoreDTO> ordersCount(
            @PathVariable("order_type") String orderType
    ){

        return orderService.getOrdersCount(orderType);

    }


}
