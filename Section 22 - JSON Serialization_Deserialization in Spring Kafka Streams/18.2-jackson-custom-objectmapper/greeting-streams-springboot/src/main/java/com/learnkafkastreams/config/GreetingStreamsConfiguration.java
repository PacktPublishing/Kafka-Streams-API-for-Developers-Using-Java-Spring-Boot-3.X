package com.learnkafkastreams.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomErrorHandler;
import com.learnkafkastreams.topology.GreetingStreamsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

@Configuration
@Slf4j
public class GreetingStreamsConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;


    @Bean
    public ObjectMapper objectMapper(){

        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }


    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        var kafkaStreamsProperties =  kafkaProperties.buildStreamsProperties();
        kafkaStreamsProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);
        kafkaStreamsProperties.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER,
                recoverer());
        return new KafkaStreamsConfiguration(kafkaStreamsProperties);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {

        return  factoryBeanConfigurer -> {
            factoryBeanConfigurer.setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());
//            factoryBeanConfigurer.set
        } ;
    }

    private ConsumerRecordRecoverer recoverer() {
        return (consumerRecord, e) -> {
            log.error("Exception is : {} , Failed Record : {} ", consumerRecord,e.getMessage(), e);
        };
    }

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
