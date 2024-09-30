package com.innotigers.controller;


import com.innotigers.model.IndexEvent;
import com.innotigers.producer.KafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/todos")
@RequiredArgsConstructor
public class IndexEventController {

    private final KafkaProducer kafkaProducer;

    @GetMapping("")
    @ResponseStatus(HttpStatus.OK)
    public IndexEvent createTodo() {
        int randomWithMathRandom = (int) ((Math.random() * (100 - 1)) + 1);
        String gcid = UUID.randomUUID().toString();
        String message = "Controller - IndexEvent Message";
        IndexEvent indexEvent = IndexEvent.builder().gcid(gcid).message(message).requestId(randomWithMathRandom).build();
        IndexEvent result =  kafkaProducer.sendToKafka(indexEvent);
        return result;
    }

}
