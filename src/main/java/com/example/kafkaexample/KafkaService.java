package com.example.kafkaexample;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class KafkaService {

    private List<String> values = new ArrayList<>(Collections.nCopies(100, null));

    public void updateValue(int id, String newValue){
        values.set(id, newValue);
    }
    public void printValues() {
        System.out.println(values);
    }
}
