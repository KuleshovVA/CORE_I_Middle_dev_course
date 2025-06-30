package spring_kafka;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import spring_kafka.config.ConsumerConfig;
import spring_kafka.config.SenderConfig;
import spring_kafka.producer.Sender;

public class App
{
    public static void main(String[] args) throws InterruptedException
    {
        System.out.println("Hello, World!");

        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext(ConsumerConfig.class, SenderConfig.class);

        Sender sender = context.getBean(Sender.class);

        sender.send("Hello! Kafka Spring Demo project!", 1);
        Thread.sleep(10);
    }
}