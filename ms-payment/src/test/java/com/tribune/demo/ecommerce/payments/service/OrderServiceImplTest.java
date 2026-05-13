package com.tribune.demo.ecommerce.payments.service;

import com.tribune.demo.ecommerce.domain.Order;
import com.tribune.demo.ecommerce.domain.OrderSource;
import com.tribune.demo.ecommerce.domain.OrderStatus;
import com.tribune.demo.ecommerce.domain.Topics;
import com.tribune.demo.ecommerce.payments.db.entity.Customer;
import com.tribune.demo.ecommerce.payments.db.repository.CustomerRepository;
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
    private CustomerRepository customerRepository;

    @Mock
    private KafkaTemplate<Long, Order> kafkaTemplate;

    @BeforeEach
    void setUp() {
        orderService = new OrderServiceImpl(customerRepository, kafkaTemplate);
    }

    @Test
    void testReserveWithSufficientFunds() {
        // Arrange
        Long customerId = 1L;
        Long orderId = 100L;
        int orderPrice = 50;

        Customer customer = new Customer();
        customer.setId(customerId);
        customer.setAmountAvailable(100);
        customer.setAmountReserved(0);

        Order order = new Order();
        order.setId(orderId);
        order.setCustomerId(customerId);
        order.setPrice(orderPrice);
        order.setStatus(OrderStatus.NEW);

        when(customerRepository.findById(customerId)).thenReturn(Optional.of(customer));
        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.ACCEPT);
        assert order.getSource().equals(OrderSource.PAYMENT);
        assert customer.getAmountAvailable() == 50; // 100 - 50
        assert customer.getAmountReserved() == 50;

        verify(customerRepository, times(1)).save(customer);
        verify(kafkaTemplate, times(1)).send(eq(Topics.PAYMENTS), eq(orderId), eq(order));
    }

    @Test
    void testReserveWithInsufficientFunds() {
        // Arrange
        Long customerId = 1L;
        Long orderId = 100L;
        int orderPrice = 150;

        Customer customer = new Customer();
        customer.setId(customerId);
        customer.setAmountAvailable(100);
        customer.setAmountReserved(0);

        Order order = new Order();
        order.setId(orderId);
        order.setCustomerId(customerId);
        order.setPrice(orderPrice);
        order.setStatus(OrderStatus.NEW);

        when(customerRepository.findById(customerId)).thenReturn(Optional.of(customer));
        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.REJECT);
        assert order.getSource().equals(OrderSource.PAYMENT);
        assert customer.getAmountAvailable() == 100; // No change
        assert customer.getAmountReserved() == 0;

        verify(customerRepository, times(1)).save(customer);
        verify(kafkaTemplate, times(1)).send(anyString(), anyLong(), any());
    }

    @Test
    void testReserveWithNullOrder() {
        // Act & Assert - Should not throw exception
        orderService.reserve(null);

        // Only verify no interactions with kafka template
        verify(kafkaTemplate, never()).send(anyString(), anyLong(), any());
        verify(customerRepository, never()).save(any());
    }

    @Test
    void testReserveWithNullCustomerId() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setCustomerId(null);
        order.setPrice(50);
        order.setStatus(OrderStatus.NEW);

        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.REJECTED);
        assert order.getSource().equals(OrderSource.PAYMENT);

        verify(customerRepository, never()).findById(any());
    }

    @Test
    void testReserveWithCustomerNotFound() {
        // Arrange
        Long customerId = 999L;
        Order order = new Order();
        order.setId(1L);
        order.setCustomerId(customerId);
        order.setPrice(50);
        order.setStatus(OrderStatus.NEW);

        when(customerRepository.findById(customerId)).thenReturn(Optional.empty());
        // Setup default mock for KafkaTemplate to return a completed future
        CompletableFuture<SendResult<Long, Order>> completedFuture =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(completedFuture);

        // Act
        orderService.reserve(order);

        // Assert
        assert order.getStatus().equals(OrderStatus.REJECTED);
        assert order.getSource().equals(OrderSource.PAYMENT);

        verify(kafkaTemplate, times(1)).send(anyString(), anyLong(), any());
    }

    @Test
    void testConfirmPayment() {
        // Arrange
        Long customerId = 1L;
        Long orderId = 100L;
        int reservedAmount = 50;

        Customer customer = new Customer();
        customer.setId(customerId);
        customer.setAmountAvailable(50);
        customer.setAmountReserved(reservedAmount);

        Order order = new Order();
        order.setId(orderId);
        order.setCustomerId(customerId);
        order.setPrice(reservedAmount);
        order.setStatus(OrderStatus.CONFIRMED);
        order.setSource(OrderSource.PAYMENT);

        when(customerRepository.findById(customerId)).thenReturn(Optional.of(customer));

        // Act
        orderService.confirm(order);

        // Assert
        assert customer.getAmountReserved() == 0; // Released
        assert customer.getAmountAvailable() == 50;

        verify(customerRepository, times(1)).save(customer);
    }

    @Test
    void testRollbackPayment() {
        // Arrange
        Long customerId = 1L;
        Long orderId = 100L;
        int reservedAmount = 50;

        Customer customer = new Customer();
        customer.setId(customerId);
        customer.setAmountAvailable(50);
        customer.setAmountReserved(reservedAmount);

        Order order = new Order();
        order.setId(orderId);
        order.setCustomerId(customerId);
        order.setPrice(reservedAmount);
        order.setStatus(OrderStatus.ROLLBACK);
        order.setSource(OrderSource.STOCK);  // From stock service

        when(customerRepository.findById(customerId)).thenReturn(Optional.of(customer));

        // Act
        orderService.confirm(order);

        // Assert
        assert customer.getAmountReserved() == 0; // Released
        assert customer.getAmountAvailable() == 100; // Refunded

        verify(customerRepository, times(1)).save(customer);
    }

    @Test
    void testConfirmWithNullOrder() {
        // Act & Assert - Should not throw exception
        orderService.confirm(null);

        verify(customerRepository, never()).save(any());
    }
}

