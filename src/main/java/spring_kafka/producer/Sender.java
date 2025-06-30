package spring_kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;

public class Sender {
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public Sender(KafkaTemplate<Integer, String> kafkaTemplate)
    {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String message, int key)
    {
        this.kafkaTemplate.send("SPRING-DEMO", key, message);
    }
}
