package com.innotigers.config;

import com.innotigers.model.IndexEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {

    @Value(value = "${BOOTSTRAP_SERVERS}")
    private String bootstrapServers;
    @Value(value = "${CONSUMER_GROUP}")
    private String groupId;
    @Value(value = "${TOPIC_NAME}")
    private String topic;
    @Value(value = "${HOUSEKEEPING_TOPIC_NAME}")
    private String hkTopic;
    @Value(value = "${PARTITION_COUNT}")
    private int partitionCount;
    @Value(value = "${REPLICA_COUNT}")
    private int replica;
    @Value(value = "${CONCURRENCY}")
    private int concurrency;
    @Value(value = "${MAX_POLL_RECORD_PER_CONSUMER}")
    private int maxPollRecordsPerConsumer;
    @Value(value = "${MAX_POLL_INTERVAL_MS}")
    private int maxPollInterval;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            KafkaTemplate<String, IndexEvent> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, IndexEvent> factory = getFactory();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setLogContainerConfig(true);
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(concurrency);
        factory.setBatchListener(true);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Moving [{}]", record);
                    return new TopicPartition(hkTopic, record.partition());
                });

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        /*
         * Set to true to commit the offset for a recovered record.
         */
        //errorHandler.setCommitRecovered(true);
        /*
         * When true (default), the error handler will perform seeks on the failed and/or remaining records to they will be redelivered on the next poll.
         * */
        //errorHandler.setSeekAfterError(false);

        //errorHandler.addRetryableExceptions(BatchListenerFailedException.class);

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Create a kafka consumer factory.
     *
     * @return Consumer factory.
     */
    @Bean
    public ConsumerFactory<String, IndexEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.innotigers");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecordsPerConsumer);
        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new JsonDeserializer<>(IndexEvent.class, false));
    }

    /**
     * Kafka admin for topic creation.
     *
     * @return KafkaAdmin to create topic.
     */
    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic indexEventTopic() {
        return TopicBuilder.name(topic)
                .partitions(partitionCount)
                .replicas(replica)
                .build();
    }

    ConcurrentKafkaListenerContainerFactory<String, IndexEvent> getFactory() {
        return new ConcurrentKafkaListenerContainerFactory<>();
    }

}
