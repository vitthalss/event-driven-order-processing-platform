package com.tribune.demo.ecommerce.payments.config;

import com.tribune.demo.ecommerce.domain.Order;
import org.jspecify.annotations.NonNull;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test configuration that provides mock beans for Kafka dependencies
 */
@TestConfiguration
public class TestKafkaConfig {

    @Bean
    public KafkaTemplate<Long, Order> kafkaTemplate() {
        KafkaTemplate<Long, Order> mockTemplate = mock(KafkaTemplate.class);
        
        // Mock send to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture = 
            CompletableFuture.completedFuture(null);
        when(mockTemplate.send(anyString(), anyLong(), any(Order.class)))
            .thenReturn(completedFuture);
        
        return mockTemplate;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Long, Order>> kafkaListenerContainerFactory() {
        return new ConcurrentKafkaListenerContainerFactory<Long, Order>() {
            @Override
            public @NonNull ConcurrentMessageListenerContainer<Long, Order> createListenerContainer(@NonNull KafkaListenerEndpoint endpoint) {
                // Return a mock container that does nothing
                ConcurrentMessageListenerContainer<Long, Order> container = mock(ConcurrentMessageListenerContainer.class);
                when(container.getPhase()).thenReturn(0);
                when(container.isAutoStartup()).thenReturn(false);
                return container;
            }
        };
    }
}

