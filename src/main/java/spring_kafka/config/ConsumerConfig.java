package spring_kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import spring_kafka.App;
import spring_kafka.consumer.Listener;
import spring_kafka.services.AdminService;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ComponentScan(basePackageClasses = App.class)
@EnableKafka
public class ConsumerConfig {

    @Bean
    public KafkaAdmin kafkaAdmin()
    {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdmin kafkaAdmin = new KafkaAdmin(config);
        kafkaAdmin.setFatalIfBrokerNotAvailable(false);
        return kafkaAdmin;
    }

    @Bean
    public NewTopic topic()
    {
        return TopicBuilder.name("SPRING_DEMO")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(10000);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory()
    {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs()
    {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    @Bean
    public Listener listener()
    {
        return new Listener();
    }
}
