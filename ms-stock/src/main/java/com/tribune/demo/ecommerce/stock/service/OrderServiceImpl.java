package com.tribune.demo.ecommerce.stock.service;

import com.tribune.demo.ecommerce.domain.Order;
import com.tribune.demo.ecommerce.domain.OrderSource;
import com.tribune.demo.ecommerce.domain.OrderStatus;
import com.tribune.demo.ecommerce.domain.Topics;
import com.tribune.demo.ecommerce.stock.db.entity.Product;
import com.tribune.demo.ecommerce.stock.db.repository.ProductRepository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Stock Service - handles inventory reservations and confirmations
 */
@Slf4j
@Service
public record OrderServiceImpl(ProductRepository repository,
                               KafkaTemplate<Long, Order> template) implements OrderService {

    private static final int KAFKA_TIMEOUT_SECONDS = 10;

    @Override
    public void reserve(Order order) {
        try {
            // Null safety check
            if (order == null) {
                log.error("Received null order for stock reservation");
                return;
            }

            log.info("Processing stock reservation for order ID: {}, product ID: {}",
                    order.getId(), order.getProductId());

            // Null safety checks
            if (order.getProductId() == null || order.getProductCount() <= 0) {
                log.error("Order has invalid product ID or count");
                order.setStatus(OrderStatus.REJECT);
                order.setSource(OrderSource.STOCK);
                sendOrderUpdate(order);
                return;
            }

            if (!OrderStatus.NEW.equals(order.getStatus())) {
                log.debug("Order not in NEW status, skipping stock reservation");
                return;
            }

            // Fetch product with null check
            Product product = repository.findById(order.getProductId())
                    .orElseGet(() -> {
                        log.warn("Product not found for ID: {}", order.getProductId());
                        return null;
                    });

            if (product == null) {
                log.error("Product not found, rejecting order: {}", order.getId());
                order.setStatus(OrderStatus.REJECT);
                order.setSource(OrderSource.STOCK);
                sendOrderUpdate(order);
                return;
            }

            log.info("Product found: {} with available items: {}",
                    product.getId(), product.getAvailableItems());

            // Process stock reservation
            if (order.getProductCount() <= product.getAvailableItems()) {
                product.setReservedItems(product.getReservedItems() + order.getProductCount());
                product.setAvailableItems(product.getAvailableItems() - order.getProductCount());
                order.setStatus(OrderStatus.ACCEPT);

                log.info("Stock reserved: {} items for order: {}", order.getProductCount(), order.getId());
            } else {
                order.setStatus(OrderStatus.REJECT);
                log.warn("Insufficient stock for order: {}. Required: {}, Available: {}",
                        order.getId(), order.getProductCount(), product.getAvailableItems());
            }

            order.setSource(OrderSource.STOCK);
            repository.save(product);
            sendOrderUpdate(order);

        } catch (Exception e) {
            log.error("Error processing stock reservation for order {}: {}",
                    order != null ? order.getId() : "unknown", e.getMessage(), e);
            if (order != null) {
                order.setStatus(OrderStatus.REJECT);
                order.setSource(OrderSource.STOCK);
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
                log.error("Received null order for stock confirmation");
                return;
            }

            log.info("Confirming stock for order ID: {}, product ID: {}",
                    order.getId(), order.getProductId());

            // Null safety checks
            if (order.getProductId() == null || order.getStatus() == null) {
                log.error("Order missing required fields for confirmation");
                return;
            }

            // Fetch product with null check
            Product product = repository.findById(order.getProductId())
                    .orElseGet(() -> {
                        log.warn("Product not found for ID: {}", order.getProductId());
                        return null;
                    });

            if (product == null) {
                log.error("Product not found for confirmation: {}", order.getId());
                return;
            }

            if (order.getStatus().equals(OrderStatus.CONFIRMED)) {
                product.setReservedItems(product.getReservedItems() - order.getProductCount());
                repository.save(product);
                log.info("Stock confirmed for order: {}", order.getId());

            } else if (order.getStatus().equals(OrderStatus.ROLLBACK) &&
                    order.getSource() != null &&
                    !order.getSource().equals(OrderSource.STOCK)) {
                product.setReservedItems(product.getReservedItems() - order.getProductCount());
                product.setAvailableItems(product.getAvailableItems() + order.getProductCount());
                repository.save(product);
                log.info("Stock rolled back for order: {}", order.getId());
            }

        } catch (Exception e) {
            log.error("Error confirming stock for order {}: {}",
                    order != null ? order.getId() : "unknown", e.getMessage(), e);
        }
    }

    /**
     * Sends order update to Kafka with timeout protection
     */
    private void sendOrderUpdate(Order order) {
        try {
            template.send(Topics.STOCK, order.getId(), order)
                    .get(KAFKA_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Order update sent to Kafka: {}", order.getId());
        } catch (Exception e) {
            log.error("Failed to send order update to Kafka: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send stock update", e);
        }
    }
}
