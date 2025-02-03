package com.learnkafkastreams.config;

import com.learnkafkastreams.topology.GreetingStreamsTopology;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class GreetingStreamsConfiguration {


    @Bean
    public NewTopic greetingsTopic(){
        return TopicBuilder.name(GreetingStreamsTopology.GREETINGS)
                .partitions(2)
                .replicas(1)
                .build();

    }

    @Bean
    public NewTopic greetingsOutputTopic(){
        return TopicBuilder.name(GreetingStreamsTopology.GREETINGS_OUTPUT)
                .partitions(2)
                .replicas(1)
                .build();

    }
}
