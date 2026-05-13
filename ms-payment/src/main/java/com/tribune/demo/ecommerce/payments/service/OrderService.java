package com.tribune.demo.ecommerce.payments.service;


import com.tribune.demo.ecommerce.domain.Order;

public interface OrderService {

    void reserve(Order order);

    void confirm(Order order);
}
