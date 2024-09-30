package com.innotigers;

import com.innotigers.model.IndexEvent;
import com.innotigers.producer.KafkaProducer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.UUID;

@SpringBootApplication
public class KafkaConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

    @Bean
    public ApplicationRunner runner(KafkaProducer kafkaProducer) {
        return args -> {
            /*
            for (int i = 1; i < 10; i++) {
                String gcid = UUID.randomUUID().toString();
                String message = "IndexEvent Message: "+i;
                IndexEvent indexEvent = IndexEvent.builder().gcid(gcid).message(message).requestId(i).build();
                kafkaProducer.sendToKafka(indexEvent);
            }
            */
        };
    }
}