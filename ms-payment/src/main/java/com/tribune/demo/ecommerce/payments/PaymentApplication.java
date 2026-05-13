package com.tribune.demo.ecommerce.payments;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;


@EnableKafka
@SpringBootApplication
public class PaymentApplication {

    static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }

}
