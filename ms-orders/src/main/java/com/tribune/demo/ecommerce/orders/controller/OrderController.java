package com.tribune.demo.ecommerce.orders.controller;

import com.tribune.demo.ecommerce.domain.Order;
import com.tribune.demo.ecommerce.domain.Topics;
import com.tribune.demo.ecommerce.orders.error.OrderNotFoundException;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;
import com.tribune.demo.ecommerce.orders.service.OrderService;

import java.util.ArrayList;
import java.util.List;


@Slf4j
@RequestMapping("/orders")
@RestController
public record OrderController(OrderService orderService,
                              StreamsBuilderFactoryBean kafkaStreamsFactory) {


    @PostMapping
    public ResponseEntity<Order> create(@Valid @RequestBody Order order) {
        try {
            log.info("Received order creation request for customer: {}", order.getCustomerId());

            Order createdOrder = orderService.createOrder(order);

            log.info("Order created successfully with ID: {}", createdOrder.getId());
            return ResponseEntity.status(HttpStatus.CREATED).body(createdOrder);

        } catch (com.tribune.demo.ecommerce.orders.error.OrderProcessingException e) {
            log.error("Order processing failed: {}", e.getMessage());
            throw e;  // Let GlobalExceptionHandler deal with it
        }
    }

    @GetMapping
    public ResponseEntity<List<Order>> all() {
        try {
            List<Order> orders = new ArrayList<>();

            // Get the queryable store with proper null checks
            if (kafkaStreamsFactory.getKafkaStreams() == null) {
                log.warn("Kafka Streams not initialized yet");
                return ResponseEntity.ok(orders);
            }

            ReadOnlyKeyValueStore<Long, Order> store = kafkaStreamsFactory
                    .getKafkaStreams()
                    .store(StoreQueryParameters.fromNameAndType(
                            Topics.ORDERS,
                            QueryableStoreTypes.keyValueStore()));

            if (store == null) {
                log.warn("State store not available");
                return ResponseEntity.ok(orders);
            }

            // Use try-with-resources to ensure iterator is closed (FIX: resource cleanup)
            try (KeyValueIterator<Long, Order> it = store.all()) {
                it.forEachRemaining(kv -> orders.add(kv.value));
            }

            log.info("Retrieved {} orders from state store", orders.size());
            return ResponseEntity.ok(orders);

        } catch (Exception e) {
            log.error("Failed to retrieve orders: {}", e.getMessage(), e);
            // Return empty list instead of throwing error for better UX
            return ResponseEntity.ok(new ArrayList<>());
        }
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<?> getOrderById(@PathVariable Long orderId) {
        if (kafkaStreamsFactory.getKafkaStreams() == null) {
            return ResponseEntity.notFound().build();
        }

        ReadOnlyKeyValueStore<Long, Order> store = kafkaStreamsFactory
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        Topics.ORDERS,
                        QueryableStoreTypes.keyValueStore()));

        Order order = store.get(orderId);

        if (order == null) {
            throw new OrderNotFoundException(orderId);
        }

        return ResponseEntity.ok(order);
    }
}
