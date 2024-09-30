/*
 *  Copyright 2021 Smarsh Inc.
 */

package com.innotigers.config;

import com.innotigers.model.IndexEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaProducerConfig {

  @Value(value = "${BOOTSTRAP_SERVERS}")
  private String bootstrapServers;

  @Value(value = "${PARTITION_COUNT}")
  private int partitionCount;

  @Value(value = "${REPLICA_COUNT}")
  private int replica;

  @Value(value = "${PRODUCER_MAX_REQUEST}")
  private int maxRequest;


  /**
   * Create a kafka consumer factory.
   *
   * @return Consumer factory.
   */
  @Bean
  public ProducerFactory<String, IndexEvent> getProducerFactory() {
    return prepareProducerFactory();
  }

  /**
   * Create a kafka producer factory.
   *
   * @return producer factory.
   */
  @Bean
  public ProducerFactory<String, IndexEvent> getProducerFactoryForIndexEvent() {
    return prepareProducerFactory();
  }

  private ProducerFactory<String, IndexEvent> prepareProducerFactory() {
    final Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxRequest);
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean("indexEventTemplate")
  public KafkaTemplate<String, IndexEvent> indexEventTemplate() {
    return new KafkaTemplate<>(getProducerFactoryForIndexEvent());
  }

}
