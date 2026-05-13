package com.tribune.demo.ecommerce.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tribune.demo.ecommerce.domain.Order;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Kafka deserializer for {@link Order} objects.
 */
public class OrderJsonDeserializer implements Deserializer<Order> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderJsonDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(java.util.Map<String, ?> configs, boolean isKey) {
        // None
    }

    @Override
    public Order deserialize(String topic, byte[] data) {
        if (data == null) {
            LOGGER.debug("Null data received for deserialization");
            return null;
        }
        try {
            return objectMapper.readValue(data, Order.class);
        } catch (Exception e) {
            LOGGER.error("Error deserializing Order from topic {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Failed to deserialize Order", e);
        }
    }

    @Override
    public void close() {
        // None
    }
}

