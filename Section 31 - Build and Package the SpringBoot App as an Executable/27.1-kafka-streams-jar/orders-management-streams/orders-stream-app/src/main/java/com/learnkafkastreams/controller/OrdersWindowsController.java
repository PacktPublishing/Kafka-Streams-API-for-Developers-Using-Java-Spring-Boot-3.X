package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrdersWindowService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
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

    @GetMapping("/windows/count")
    public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountByWindows(
            @RequestParam(value = "from_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime fromTime,
            @RequestParam(value = "to_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime toTime
    ){
        if(fromTime!=null && toTime!=null){
            return ordersWindowService.getAllOrdersCountByWindows(fromTime, toTime);
        }
        return ordersWindowService.getAllOrdersCountByWindows();

    }

    @GetMapping("/windows/revenue/{order_type}")
    public List<OrdersRevenuePerStoreByWindowsDTO> ordersRevenue(
            @PathVariable("order_type") String orderType
    ){
        return ordersWindowService.getOrdersRevenueWindowsByType(orderType);

    }
}
