package spring_kafka;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import spring_kafka.config.ConsumerConfig;
import spring_kafka.config.SenderConfig;
import spring_kafka.producer.Sender;
import spring_kafka.services.AdminService;

public class App
{
    public static void main(String[] args) throws InterruptedException
    {
        System.out.println("Hello, World!");

        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext(ConsumerConfig.class, SenderConfig.class);

        createNewTopic("NEW-TOPIC", context.getBean(AdminService.class));

        Sender sender = context.getBean(Sender.class);

        sender.send("Hello! Kafka Spring Demo project!", 1);
        Thread.sleep(10);
    }

    public static void createNewTopic(String topicName, AdminService adminService)
    {
        System.out.println(adminService.getTopicInfo(topicName));
        adminService.createNewTopic(topicName);
        System.out.println(adminService.getTopicInfo(topicName));
    }
}