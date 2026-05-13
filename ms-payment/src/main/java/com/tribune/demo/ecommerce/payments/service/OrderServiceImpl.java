package com.tribune.demo.ecommerce.payments.service;


import com.tribune.demo.ecommerce.domain.Order;
import com.tribune.demo.ecommerce.domain.OrderSource;
import com.tribune.demo.ecommerce.domain.OrderStatus;
import com.tribune.demo.ecommerce.domain.Topics;
import com.tribune.demo.ecommerce.payments.db.entity.Customer;
import com.tribune.demo.ecommerce.payments.db.repository.CustomerRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Payment Service - handles payment reservations and confirmations
 */
@Slf4j
@Service
public record OrderServiceImpl(CustomerRepository repository,
                               KafkaTemplate<Long, Order> template) implements OrderService {

    private static final int KAFKA_TIMEOUT_SECONDS = 10;

    @Override
    public void reserve(Order order) {
        try {
            // Null safety check
            if (order == null) {
                log.error("Received null order for payment reservation");
                return;
            }

            log.info("Processing payment reservation for order ID: {}, customer ID: {}",
                    order.getId(), order.getCustomerId());

            // Null safety check
            if (order.getCustomerId() == null) {
                log.error("Order has null customerId");
                order.setStatus(OrderStatus.REJECTED);
                order.setSource(OrderSource.PAYMENT);
                sendOrderUpdate(order);
                return;
            }

            // Fetch customer with null check
            Customer customer = repository.findById(order.getCustomerId())
                    .orElseGet(() -> {
                        log.warn("Customer not found for ID: {}", order.getCustomerId());
                        return null;
                    });

            if (customer == null) {
                log.error("Customer not found, rejecting order: {}", order.getId());
                order.setStatus(OrderStatus.REJECTED);
                order.setSource(OrderSource.PAYMENT);
                sendOrderUpdate(order);
                return;
            }

            log.info("Customer found: {} with available amount: {}",
                    customer.getId(), customer.getAmountAvailable());

            // Process payment reservation
            if (order.getPrice() > 0 && order.getPrice() < customer.getAmountAvailable()) {
                order.setStatus(OrderStatus.ACCEPT);
                customer.setAmountReserved(customer.getAmountReserved() + order.getPrice());
                customer.setAmountAvailable(customer.getAmountAvailable() - order.getPrice());

                log.info("Payment reserved: {} for order: {}", order.getPrice(), order.getId());
            } else {
                order.setStatus(OrderStatus.REJECT);
                log.warn("Insufficient funds for order: {}. Required: {}, Available: {}",
                        order.getId(), order.getPrice(), customer.getAmountAvailable());
            }

            order.setSource(OrderSource.PAYMENT);
            repository.save(customer);
            sendOrderUpdate(order);

        } catch (Exception e) {
            log.error("Error processing payment reservation for order {}: {}",
                    order != null ? order.getId() : "unknown", e.getMessage(), e);
            if (order != null) {
                order.setStatus(OrderStatus.REJECTED);
                order.setSource(OrderSource.PAYMENT);
                try {
                    sendOrderUpdate(order);
                } catch (Exception ex) {
                    log.error("Failed to send rejection message: {}", ex.getMessage());
                }
            }
        }
    }

    @Override
    public void confirm(Order order) {
        try {
            // Null safety check
            if (order == null) {
                log.error("Received null order for payment confirmation");
                return;
            }

            log.info("Confirming payment for order ID: {}, customer ID: {}",
                    order.getId(), order.getCustomerId());

            // Null safety check
            if (order.getCustomerId() == null || order.getStatus() == null) {
                log.error("Order missing required fields for confirmation");
                return;
            }

            // Fetch customer with null check
            Customer customer = repository.findById(order.getCustomerId())
                    .orElseGet(() -> {
                        log.warn("Customer not found for ID: {}", order.getCustomerId());
                        return null;
                    });

            if (customer == null) {
                log.error("Customer not found for confirmation: {}", order.getId());
                return;
            }

            if (order.getStatus().equals(OrderStatus.CONFIRMED)) {
                customer.setAmountReserved(customer.getAmountReserved() - order.getPrice());
                repository.save(customer);
                log.info("Payment confirmed and amount released for order: {}", order.getId());

            } else if (order.getStatus().equals(OrderStatus.ROLLBACK) &&
                    order.getSource() != null &&
                    !order.getSource().equals(OrderSource.PAYMENT)) {
                customer.setAmountReserved(customer.getAmountReserved() - order.getPrice());
                customer.setAmountAvailable(customer.getAmountAvailable() + order.getPrice());
                repository.save(customer);
                log.info("Payment rolled back for order: {}", order.getId());
            }

        } catch (Exception e) {
            log.error("Error confirming payment for order {}: {}",
                    order != null ? order.getId() : "unknown", e.getMessage(), e);
        }
    }

    /**
     * Sends order update to Kafka with timeout protection
     */
    private void sendOrderUpdate(Order order) {
        try {
            template.send(Topics.PAYMENTS, order.getId(), order)
                    .get(KAFKA_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Order update sent to Kafka: {}", order.getId());
        } catch (Exception e) {
            log.error("Failed to send order update to Kafka: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send payment update", e);
        }
    }
}
