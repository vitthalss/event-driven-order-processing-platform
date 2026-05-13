package com.tribune.demo.ecommerce.stock;

import com.tribune.demo.ecommerce.stock.config.TestKafkaConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(TestKafkaConfig.class)
@ActiveProfiles("test")
class StockApplicationTests {

    @Test
    void contextLoads() {
    }

}
