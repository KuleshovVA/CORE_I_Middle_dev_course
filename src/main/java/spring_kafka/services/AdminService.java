package spring_kafka.services;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
public class AdminService
{
    private final KafkaAdmin kafkaAdmin;

    public AdminService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    public void createNewTopic(String topicName)
    {
        System.out.println("Create new topic: " + topicName);
        NewTopic newTopic = TopicBuilder.name(topicName)
            .partitions(10)
            .replicas(1)
            .build();
        kafkaAdmin.createOrModifyTopics(newTopic);
    }

    public String getTopicInfo(String topicName)
    {
        try
        {
            return this.kafkaAdmin.describeTopics(topicName).toString();
        }
        catch (KafkaException e)
        {
            return e.getMessage();
        }
    }
}
