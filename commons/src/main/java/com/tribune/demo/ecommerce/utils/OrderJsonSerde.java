package com.tribune.demo.ecommerce.utils;

import com.tribune.demo.ecommerce.domain.Order;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Custom Serde (Serializer/Deserializer) for {@link Order} objects.
 * Used within Kafka Streams
 */
public class OrderJsonSerde implements Serde<Order> {

    @Override
    public void configure(java.util.Map<String, ?> configs, boolean isKey) {
        // None
    }

    @Override
    public void close() {
        // None
    }

    @Override
    public Serializer<Order> serializer() {
        return new OrderJsonSerializer();
    }

    @Override
    public Deserializer<Order> deserializer() {
        return new OrderJsonDeserializer();
    }
}

