package com.tribune.demo.ecommerce.stock.service;

import com.tribune.demo.ecommerce.domain.Order;
import com.tribune.demo.ecommerce.domain.OrderSource;
import com.tribune.demo.ecommerce.domain.OrderStatus;
import com.tribune.demo.ecommerce.domain.Topics;
import com.tribune.demo.ecommerce.stock.db.entity.Product;
import com.tribune.demo.ecommerce.stock.db.repository.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class OrderServiceImplTest {

    private OrderServiceImpl orderService;

    @Mock
    private ProductRepository productRepository;

    @Mock
    private KafkaTemplate<Long, Order> kafkaTemplate;

    @BeforeEach
    void setUp() {
        orderService = new OrderServiceImpl(productRepository, kafkaTemplate);
    }

    @Test
    void testReserveWithSufficientStock() {
        // Arrange
        Long productId = 1L;
        Long orderId = 100L;
        int orderQuantity = 5;

        Product product = new Product();
        product.setId(productId);
        product.setAvailableItems(100);
        product.setReservedItems(0);

        Order order = new Order();
        order.setId(orderId);
        order.setProductId(productId);
        order.setProductCount(orderQuantity);
        order.setStatus(OrderStatus.NEW);

        when(productRepository.findById(productId)).thenReturn(Optional.of(product));
        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.ACCEPT);
        assert order.getSource().equals(OrderSource.STOCK);
        assert product.getAvailableItems() == 95; // 100 - 5
        assert product.getReservedItems() == 5;

        verify(productRepository, times(1)).save(product);
        verify(kafkaTemplate, times(1)).send(eq(Topics.STOCK), eq(orderId), eq(order));
    }

    @Test
    void testReserveWithInsufficientStock() {
        // Arrange
        Long productId = 1L;
        Long orderId = 100L;
        int orderQuantity = 150;

        Product product = new Product();
        product.setId(productId);
        product.setAvailableItems(100);
        product.setReservedItems(0);

        Order order = new Order();
        order.setId(orderId);
        order.setProductId(productId);
        order.setProductCount(orderQuantity);
        order.setStatus(OrderStatus.NEW);

        when(productRepository.findById(productId)).thenReturn(Optional.of(product));
        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.REJECT);
        assert order.getSource().equals(OrderSource.STOCK);
        assert product.getAvailableItems() == 100; // No change
        assert product.getReservedItems() == 0;

        verify(productRepository, times(1)).save(product);//saved as REJECTED
        verify(kafkaTemplate, times(1)).send(anyString(), anyLong(), any());
    }

    @Test
    void testReserveWithNullOrder() {
        // Act & Assert - Should not throw exception
        orderService.reserve(null);

        // Verify no interactions
        verify(kafkaTemplate, never()).send(anyString(), anyLong(), any());
        verify(productRepository, never()).save(any());
    }

    @Test
    void testReserveWithNullProductId() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(null);
        order.setProductCount(5);
        order.setStatus(OrderStatus.NEW);

        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.REJECT);
        assert order.getSource().equals(OrderSource.STOCK);

        verify(productRepository, never()).findById(any());
    }

    @Test
    void testReserveWithZeroQuantity() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(1L);
        order.setProductCount(0);
        order.setStatus(OrderStatus.NEW);

        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.REJECT);

        verify(productRepository, never()).findById(any());
    }

    @Test
    void testReserveWithProductNotFound() {
        // Arrange
        Long productId = 999L;
        Order order = new Order();
        order.setId(1L);
        order.setProductId(productId);
        order.setProductCount(5);
        order.setStatus(OrderStatus.NEW);

        when(productRepository.findById(productId)).thenReturn(Optional.empty());
        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);


        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.REJECT);
        assert order.getSource().equals(OrderSource.STOCK);

        verify(kafkaTemplate, times(1)).send(anyString(), anyLong(), any());
    }

    @Test
    void testReserveWithNonNewStatus() {
        // Arrange - Order already has a status (not NEW)
        Long productId = 1L;
        Order order = new Order();
        order.setId(1L);
        order.setProductId(productId);
        order.setProductCount(5);
        order.setStatus(OrderStatus.CONFIRMED);

        // Act
        orderService.reserve(order);

        // Assert - Should skip processing
        verify(productRepository, never()).save(any());
        verify(kafkaTemplate, never()).send(anyString(), anyLong(), any());
    }

    @Test
    void testConfirmStock() {
        // Arrange
        Long productId = 1L;
        Long orderId = 100L;
        int reservedQuantity = 5;

        Product product = new Product();
        product.setId(productId);
        product.setAvailableItems(95);
        product.setReservedItems(reservedQuantity);

        Order order = new Order();
        order.setId(orderId);
        order.setProductId(productId);
        order.setProductCount(reservedQuantity);
        order.setStatus(OrderStatus.CONFIRMED);
        order.setSource(OrderSource.STOCK);

        when(productRepository.findById(productId)).thenReturn(Optional.of(product));

        // Act
        orderService.confirm(order);

        // Assert
        assert product.getReservedItems() == 0; // Released
        assert product.getAvailableItems() == 95;

        verify(productRepository, times(1)).save(product);
    }

    @Test
    void testRollbackStock() {
        // Arrange
        Long productId = 1L;
        Long orderId = 100L;
        int reservedQuantity = 5;

        Product product = new Product();
        product.setId(productId);
        product.setAvailableItems(95);
        product.setReservedItems(reservedQuantity);

        Order order = new Order();
        order.setId(orderId);
        order.setProductId(productId);
        order.setProductCount(reservedQuantity);
        order.setStatus(OrderStatus.ROLLBACK);
        order.setSource(OrderSource.PAYMENT);  // From payment service

        when(productRepository.findById(productId)).thenReturn(Optional.of(product));

        // Act
        orderService.confirm(order);

        // Assert
        assert product.getReservedItems() == 0; // Released
        assert product.getAvailableItems() == 100; // Refunded

        verify(productRepository, times(1)).save(product);
    }

    @Test
    void testConfirmWithNullOrder() {
        // Act & Assert - Should not throw exception
        orderService.confirm(null);

        verify(productRepository, never()).save(any());
    }
}

