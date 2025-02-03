package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.TotalRevenue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class OrderStoreService {

    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public OrderStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public ReadOnlyKeyValueStore<String, Long> ordersCountStore(String storeName) {

        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(

                        storeName,
                        QueryableStoreTypes.keyValueStore()
                ));

    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> ordersRevenueStore(String storeName) {

        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.keyValueStore()
                ));

    }
}
