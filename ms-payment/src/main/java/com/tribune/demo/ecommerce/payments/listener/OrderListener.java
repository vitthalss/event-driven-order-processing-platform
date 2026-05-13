package com.tribune.demo.ecommerce.payments.listener;


import com.tribune.demo.ecommerce.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.tribune.demo.ecommerce.payments.service.OrderService;

/**
 * Payment Order Listener - listens to order events from Kafka
 */
@Slf4j
@Component
public record OrderListener(OrderService orderService) {


    @KafkaListener(id = KafkaIds.ORDERS, topics = Topics.ORDERS, groupId = KafkaGroupIds.PAYMENTS)
    public void onEvent(Order o) {
        try {
            // Null safety check
            if (o == null) {
                log.error("Received null order event");
                return;
            }

            log.info("Received order event: ID={}, Status={}, CustomerId={}",
                    o.getId(), o.getStatus(), o.getCustomerId());

            // Null safety check for order status
            if (o.getStatus() == null) {
                log.error("Order has null status, skipping processing: {}", o.getId());
                return;
            }

            if (OrderStatus.NEW.equals(o.getStatus())) {
                log.debug("Processing new order for payment: {}", o.getId());
                orderService.reserve(o);
            } else {
                log.debug("Processing confirmation for order: {}", o.getId());
                orderService.confirm(o);
            }

            log.info("Order processed successfully: {}", o.getId());

        } catch (Exception e) {
            log.error("Error processing order event: {}", e.getMessage(), e);
            // Don't rethrow - allow Kafka to move on to next message
        }
    }
}

