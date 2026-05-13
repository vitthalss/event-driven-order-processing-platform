package com.tribune.demo.ecommerce.stock.db.repository;

import com.tribune.demo.ecommerce.stock.db.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}
