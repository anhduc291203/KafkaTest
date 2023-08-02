package com.example.kafkaexample;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {

    private KafkaService kafkaService;

    public KafkaListeners(KafkaService kafkaService){
        this.kafkaService = kafkaService;
    }

    @KafkaListener(topics = "Mika", groupId = "groupID")
    void listener(String data){
        String[] parts = data.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid message format");
        }

        int id = Integer.parseInt(parts[0]);
        String newValue = parts[1];

        kafkaService.updateValue(id, newValue);
        kafkaService.printValues();

        System.out.println("Listener received updated value from id " + id + ": " + newValue);
    }
}
