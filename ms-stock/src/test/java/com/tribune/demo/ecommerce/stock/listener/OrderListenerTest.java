package com.tribune.demo.ecommerce.stock.listener;

import com.tribune.demo.ecommerce.domain.Order;
import com.tribune.demo.ecommerce.domain.OrderStatus;
import com.tribune.demo.ecommerce.stock.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class OrderListenerTest {

    private OrderListener orderListener;

    @Mock
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderListener = new OrderListener(orderService);
    }

    @Test
    void testOnEventWithNewOrder() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(50L);
        order.setStatus(OrderStatus.NEW);
        order.setProductCount(5);

        // Act
        orderListener.onEvent(order);

        // Assert
        verify(orderService, times(1)).reserve(order);
        verify(orderService, never()).confirm(any());
    }

    @Test
    void testOnEventWithConfirmationOrder() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(50L);
        order.setStatus(OrderStatus.CONFIRMED);
        order.setProductCount(5);

        // Act
        orderListener.onEvent(order);

        // Assert
        verify(orderService, never()).reserve(any());
        verify(orderService, times(1)).confirm(order);
    }

    @Test
    void testOnEventWithNullOrder() {
        // Act - Should not throw exception
        orderListener.onEvent(null);

        // Assert - No service calls
        verify(orderService, never()).reserve(any());
        verify(orderService, never()).confirm(any());
    }

    @Test
    void testOnEventWithNullStatus() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(50L);
        order.setStatus(null);  // Null status
        order.setProductCount(5);

        // Act - Should not throw exception
        orderListener.onEvent(order);

        // Assert - No service calls
        verify(orderService, never()).reserve(any());
        verify(orderService, never()).confirm(any());
    }

    @Test
    void testOnEventWithServiceException() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(50L);
        order.setStatus(OrderStatus.NEW);
        order.setProductCount(5);

        // Make reserve throw exception
        doThrow(new RuntimeException("Service error")).when(orderService).reserve(any());

        // Act - Should not throw exception
        orderListener.onEvent(order);

        // Assert - Exception was logged but didn't stop processing
        verify(orderService, times(1)).reserve(order);
    }

    @Test
    void testOnEventWithRejectedStatus() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(50L);
        order.setStatus(OrderStatus.REJECT);
        order.setProductCount(5);

        // Act
        orderListener.onEvent(order);

        // Assert - REJECT is not NEW, so should call confirm
        verify(orderService, never()).reserve(any());
        verify(orderService, times(1)).confirm(order);
    }

    @Test
    void testOnEventWithRollbackStatus() {
        // Arrange
        Order order = new Order();
        order.setId(1L);
        order.setProductId(50L);
        order.setStatus(OrderStatus.ROLLBACK);
        order.setProductCount(5);

        // Act
        orderListener.onEvent(order);

        // Assert - ROLLBACK is not NEW, so should call confirm
        verify(orderService, never()).reserve(any());
        verify(orderService, times(1)).confirm(order);
    }
}

