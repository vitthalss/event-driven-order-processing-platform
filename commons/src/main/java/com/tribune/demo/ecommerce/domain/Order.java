package com.tribune.demo.ecommerce.domain;


import lombok.*;
import jakarta.validation.constraints.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {
    private Long id;

    @NotNull(message = "customerId cannot be null")
    @Positive(message = "customerId must be positive")
    private Long customerId;

    @NotNull(message = "productId cannot be null")
    @Positive(message = "productId must be positive")
    private Long productId;

    @NotNull(message = "productCount cannot be null")
    @Positive(message = "productCount must be greater than 0")
    private int productCount;

    @NotNull(message = "price cannot be null")
    @Positive(message = "price must be greater than 0")
    private int price;

    private OrderStatus status;
    private OrderSource source;

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", customerId=" + customerId +
                ", productId=" + productId +
                ", productCount=" + productCount +
                ", price=" + price +
                ", status=" + status +
                ", source=" + source +
                '}';
    }
}