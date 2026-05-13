package com.tribune.demo.ecommerce.orders.config;


import com.tribune.demo.ecommerce.domain.KafkaIds;
import com.tribune.demo.ecommerce.domain.Order;
import com.tribune.demo.ecommerce.domain.Topics;
import com.tribune.demo.ecommerce.orders.service.OrderService;
import com.tribune.demo.ecommerce.utils.OrderJsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.tribune.demo.ecommerce.domain.Topics.ORDERS;
import static org.apache.kafka.streams.StreamsConfig.*;


@Slf4j
@RequiredArgsConstructor
@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {

    private final OrderService orderService;

    @Bean(name = "defaultKafkaStreamsConfig")
    public KafkaStreamsConfiguration kStreamsConfig(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        log.info("Configuring Kafka Streams");
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "ms-orders-streams-app");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, OrderJsonSerde.class);
        props.put(NUM_STREAM_THREADS_CONFIG, 3);

        return new KafkaStreamsConfiguration(props);
    }

    //this is the one that will get results from other topics to check
    @Bean
    public KStream<Long, Order> stream(StreamsBuilder builder) {
        //provides serialization and deserialization in JSON format
        // Using custom OrderJsonSerde from commons module

        Serde<Long> keySerde = Serdes.Long();
        Serde<Order> valueSerde = new OrderJsonSerde();

        // Kafka stream => it's a record stream that represents key & value pairs
        // key and value serdes means Key & value serializers & deserializers
        //of course, the key in our case is the order id
        KStream<Long, Order> paymentStream = builder
                .stream(Topics.PAYMENTS, Consumed.with(keySerde, valueSerde));//Consumed With == passing some parameters for configuring the generated stream

        KStream<Long, Order> stockStream = builder
                .stream(Topics.STOCK, Consumed.with(keySerde, valueSerde));

        //join records from both tables
        paymentStream.join(
                        stockStream,
                        orderService::confirm,//the value joiner == the one responsible for joining the two records
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)), // timestamps of matched records must fall within this window of time
                        StreamJoined.with(keySerde, valueSerde, valueSerde)//the key must be the same, 1st stream serde, 2nd stream serde
                )
                .peek((k, v) -> log.info("Kafka stream match: key[{}],value[{}]", k, v))
                .to(ORDERS);

        return paymentStream;
    }

    /**
     * To build a persistent key-value store
     * This KTable will be used to store all the Orders
     ***/

    @Bean
    public KTable<Long, Order> table(StreamsBuilder builder) {

        KeyValueBytesStoreSupplier store = Stores.persistentKeyValueStore(KafkaIds.ORDERS);

        Serde<Long> keySerde = Serdes.Long();
        Serde<Order> valueSerde = new OrderJsonSerde();

        KStream<Long, Order> stream = builder
                .stream(ORDERS, Consumed.with(keySerde, valueSerde))
                .peek((k, v) -> log.info("Kafka persistence table: key[{}],value[{}]", k, v));

        return stream.toTable(Materialized.<Long, Order>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));
    }
}
