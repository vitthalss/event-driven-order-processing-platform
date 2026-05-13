package com.tribune.demo.ecommerce.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@EnableKafkaStreams
@SpringBootApplication
public class OrderApplication {

    static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }

}
