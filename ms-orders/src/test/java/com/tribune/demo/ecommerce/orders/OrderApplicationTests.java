package com.tribune.demo.ecommerce.orders;

import com.tribune.demo.ecommerce.orders.config.TestKafkaConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(TestKafkaConfig.class)
@ActiveProfiles("test")
class OrderApplicationTests {

    @Test
    void contextLoads() {
    }

}
