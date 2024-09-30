package com.innotigers.producer;

import com.innotigers.model.IndexEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class KafkaProducer {

    @Value(value = "${TOPIC_NAME}")
    private String topic;

    @Value(value = "${KAFKA_WRITE_TIMEOUT_SECS}")
    private Long kafkaWriteTimeout;

    private final KafkaTemplate<String, IndexEvent> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, IndexEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public IndexEvent sendToKafka(IndexEvent indexEvent) {
        IndexEvent response = null;
        try {
            CompletableFuture<SendResult<String, IndexEvent>> kafkaAck = kafkaTemplate.send(topic, indexEvent);
            final SendResult<String, IndexEvent> publishResult = kafkaAck.get(kafkaWriteTimeout, TimeUnit.SECONDS);
            response = publishResult.getProducerRecord().value();
            log.info("Successfully published message to Kafka at offset [{}] in topic [{}]",
                    publishResult.getRecordMetadata().offset(), publishResult.getProducerRecord().topic());
        } catch (Exception e) {
            log.error("Error while sending to kafka. Error: ", e.getMessage());
        }
        return response;
    }

}
