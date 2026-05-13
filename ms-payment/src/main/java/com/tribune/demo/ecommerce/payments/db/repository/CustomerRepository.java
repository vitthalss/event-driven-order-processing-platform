package com.tribune.demo.ecommerce.payments.db.repository;

import com.tribune.demo.ecommerce.payments.db.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Long> {
}