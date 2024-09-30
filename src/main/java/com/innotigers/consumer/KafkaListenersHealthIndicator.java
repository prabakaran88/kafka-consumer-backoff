package com.innotigers.consumer;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@RequiredArgsConstructor
@Component
public class KafkaListenersHealthIndicator extends AbstractHealthIndicator {

    @Autowired
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        Map<String, ConsumerHealth> consumerHealths = new TreeMap<>(Comparator.naturalOrder());
        final Collection<MessageListenerContainer> allListenerContainers = kafkaListenerEndpointRegistry
                .getAllListenerContainers();
        Status allConsumerStatus = Status.UP;

        for (MessageListenerContainer messageListenerContainer : allListenerContainers) {
            if (!messageListenerContainer.isRunning()) {
                allConsumerStatus = Status.DOWN;
            }
            consumerHealths.put(messageListenerContainer.getListenerId(), buildConsumerHealth(messageListenerContainer));
        }

        builder.status(allConsumerStatus).withDetails(consumerHealths).build();
    }

    private ConsumerHealth buildConsumerHealth(MessageListenerContainer messageListenerContainer) {
        Set<String> assignedPartitions = new HashSet<>();
        messageListenerContainer.getAssignedPartitions().forEach(e -> assignedPartitions.add(e.topic() + ":" + e.partition()));
        return ConsumerHealth.builder()
                .status(messageListenerContainer.isRunning() ? Status.UP : Status.DOWN)
                .groupId(messageListenerContainer.getGroupId())
                .build();
    }

    @Builder
    @Getter
    @Setter
    private static class ConsumerHealth {
        private Status status;
        private String groupId;
        private Set<String> assignedPartitions = new HashSet<>();
    }
}