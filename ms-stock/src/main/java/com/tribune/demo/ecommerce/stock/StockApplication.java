package com.tribune.demo.ecommerce.stock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class StockApplication {

    static void main(String[] args) {
        SpringApplication.run(StockApplication.class, args);
    }

}
