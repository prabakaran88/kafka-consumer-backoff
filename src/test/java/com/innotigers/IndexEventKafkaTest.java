package com.innotigers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.innotigers.model.IndexEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
public class IndexEventKafkaTest extends IntegrationTest {

    @Autowired
    private KafkaTemplate<String, IndexEvent> kafkaTemplate;
    protected static final ObjectMapper mapper = new ObjectMapper();
    @Value(value = "${TOPIC_NAME}")
    private String topicName;
    @Value(value = "${HOUSEKEEPING_TOPIC_NAME}")
    private String hkTopicName;
    @Value(value = "${CONSUMER_GROUP}")
    private String consumerGroup;

    @Value(value = "${KAFKA_WRITE_TIMEOUT_SECS}")
    private Long kafkaWriteTimeout;

    protected static KafkaMessageListenerContainer<String, IndexEvent> container;
    protected static KafkaMessageListenerContainer<String, IndexEvent> hkListnerContainer;
    private BlockingQueue<ConsumerRecord<String, IndexEvent>> records = new LinkedBlockingQueue<>();
    private BlockingQueue<ConsumerRecord<String, IndexEvent>> hkRecords = new LinkedBlockingQueue<>();
    private KafkaConsumer<String, String> consumer;
    private AdminClient adminClient;

    @BeforeClass
    private void init() {
        //kafka setup
        setKafkaProps();
        setHouseKeepingKafkaProps();
        adminClient = getAdminClient(kafkaContainer.getBootstrapServers());
        consumer = getKafkaConsumer(kafkaContainer.getBootstrapServers());
    }

    private void setKafkaProps() {
        ContainerProperties containerProperties = new ContainerProperties(topicName);
        JsonDeserializer<IndexEvent> deserializer = new JsonDeserializer<>(
                IndexEvent.class,
                false);
        deserializer.addTrustedPackages("com.innotigers");
        DefaultKafkaConsumerFactory<String, IndexEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(getConsumerProperties(),
                        new StringDeserializer(), deserializer);

        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener(
                (MessageListener<String, IndexEvent>) records::add);
        container.start();
    }

    private void setHouseKeepingKafkaProps() {
        ContainerProperties containerProperties = new ContainerProperties(hkTopicName);
        JsonDeserializer<IndexEvent> deserializer = new JsonDeserializer<>(
                IndexEvent.class,
                false);
        deserializer.addTrustedPackages("com.innotigers");
        DefaultKafkaConsumerFactory<String, IndexEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(getConsumerProperties(),
                        new StringDeserializer(), deserializer);

        hkListnerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        hkRecords = new LinkedBlockingQueue<>();
        hkListnerContainer.setupMessageListener(
                (MessageListener<String, IndexEvent>) hkRecords::add);
        hkListnerContainer.start();
    }


    private Map<TopicPartition, Long> getProducerOffsets(
            Map<TopicPartition, Long> consumerGrpOffset) {
        List<TopicPartition> topicPartitions = new LinkedList<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
            TopicPartition key = entry.getKey();
            topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
        }
        return consumer.endOffsets(topicPartitions);
    }

    private KafkaConsumer<String, String> getKafkaConsumer(String bootstrapServerConfig) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    public Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId)
            throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> metadataMap
                = info.partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, Long> groupOffset = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : metadataMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndMetadata metadata = entry.getValue();
            groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
        }
        return groupOffset;
    }

    private AdminClient getAdminClient(String bootstrapServerConfig) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
        return AdminClient.create(config);
    }

    public Map<TopicPartition, Long> computeLags(
            Map<TopicPartition, Long> consumerGrpOffsets,
            Map<TopicPartition, Long> producerOffsets) {
        Map<TopicPartition, Long> lags = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(Math.max(0, producerOffset) - Math.max(0, consumerOffset));
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
    }

    @Test(description = "send messages to kafka and verify its consumed and processed.")
    public void testKafkaMessage_processed_successfully() throws InterruptedException, ExecutionException, TimeoutException {

        for (int i = 0; i <= 4; i++) {
            String gcid = UUID.randomUUID().toString();
            String message = "IndexEvent Message: " + i;
            IndexEvent indexEvent = IndexEvent.builder().gcid(gcid).message(message).requestId(i).build();

            CompletableFuture<SendResult<String, IndexEvent>> kafkaAck = kafkaTemplate.send(topicName, indexEvent);
            final SendResult<String, IndexEvent> publishResult = kafkaAck.get(kafkaWriteTimeout, TimeUnit.SECONDS);
            IndexEvent response = publishResult.getProducerRecord().value();
            log.info("Integration test successfully published message [{}] to Kafka at offset [{}] in topic [{}] ",
                    response, publishResult.getRecordMetadata().offset(), publishResult.getProducerRecord().topic());

            ConsumerRecord<String, IndexEvent> received = records.poll(10, TimeUnit.SECONDS);
            Assert.assertNotNull(received.value());
            Assert.assertEquals(received.value().getRequestId(), i);
        }

        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    //Checking indexEvent topic is empty
                    Assert.assertTrue(getTopicLag().isEmpty());
                });
    }

    private List<Map.Entry<TopicPartition, Long>> getTopicLag() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(consumerGroup);
        Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
        Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);
        return lags.entrySet().stream().filter(x -> x.getValue() > 0).toList();
    }

}
