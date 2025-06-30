package spring_kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;

public class Listener
{
    @KafkaListener(id="listen1", topics="SPRING-DEMO")
    public void listen(String message)
    {
        System.out.println("Listen: " + message);
    }
}
