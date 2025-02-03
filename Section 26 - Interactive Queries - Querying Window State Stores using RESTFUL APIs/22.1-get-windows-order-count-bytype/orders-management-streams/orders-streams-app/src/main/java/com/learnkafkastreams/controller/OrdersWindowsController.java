package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrdersWindowService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/orders")
public class OrdersWindowsController {

    private final OrdersWindowService ordersWindowService;

    public OrdersWindowsController(OrdersWindowService ordersWindowService) {
        this.ordersWindowService = ordersWindowService;
    }

    @GetMapping("/windows/count/{order_type}")
    public List<OrdersCountPerStoreByWindowsDTO> ordersCount(
            @PathVariable("order_type") String orderType
    ){
        return ordersWindowService.getOrdersCountWindowsByType(orderType);

    }
}
