package com.tribune.demo.ecommerce.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tribune.demo.ecommerce.domain.Order;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Kafka serializer for {@link Order} objects.
 */
public class OrderJsonSerializer implements Serializer<Order> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderJsonSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(java.util.Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public byte[] serialize(String topic, Order data) {
        if (data == null) {
            LOGGER.debug("Null data to serialize for topic {}", topic);
            return new byte[0];
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            LOGGER.error("Error serializing Order for topic {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Failed to serialize Order", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}

