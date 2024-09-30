package com.innotigers.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.innotigers.model.IndexEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Component
@Slf4j
public class KafkaConsumer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${TOPIC_NAME}",
            groupId = "${CONSUMER_GROUP}", containerFactory = "kafkaListenerContainerFactory", autoStartup = "false")
    public void consume(@Payload List<IndexEvent> messages,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
                        @Header(KafkaHeaders.OFFSET) List<Long> offsets,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
                        Acknowledgment acknowledgment) throws JsonProcessingException {
        log.info("Received a indexEvent message key/id {}, date {}, partition {}", messages, new Date(), partitions);


        for (int i = 0; i < messages.size(); i++) {
            IndexEvent request = messages.get(i);
            log.info("Received message "+objectMapper.writeValueAsString(request));

            if(request.getRequestId() == 5) {
                throw new RuntimeException("unhandled exception");
            }
        }
        log.info("Invoking acknowledgement");
        acknowledgment.acknowledge();
    }


}
