package com.tribune.demo.ecommerce.orders.service;


import com.tribune.demo.ecommerce.domain.Order;

public interface OrderService {

    Order confirm(Order orderPayment, Order orderStock);
    
    /**
     * Create a new order and send it to Kafka
     * @param order the order to create
     * @return the created order
     */
    Order createOrder(Order order);
}