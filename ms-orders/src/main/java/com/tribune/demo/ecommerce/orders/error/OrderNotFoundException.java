package com.tribune.demo.ecommerce.orders.error;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderNotFoundException extends RuntimeException {

    private final Object orderId;

    public OrderNotFoundException(Object orderId) {
        this.orderId = orderId;
        super("Order not found with ID: " + orderId);
    }
}

