package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    static public Serde<Order> orderSerdes() {
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    static public Serde<Revenue> revenueSerdes() {
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Revenue> jsonDeserializer = new JsonDeserializer<>(Revenue.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<TotalRevenue> totalRevenueSerdes() {
        JsonSerializer<TotalRevenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<TotalRevenue> jsonDeserializer = new JsonDeserializer<>(TotalRevenue.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


    }

    public static Serde<TotalRevenueWithAddress> totalRevenueWithAddressSerdes() {
        JsonSerializer<TotalRevenueWithAddress> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<TotalRevenueWithAddress> jsonDeserializer = new JsonDeserializer<>(TotalRevenueWithAddress.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    }

    public static Serde<Store>  storeSerdes() {
        JsonSerializer<Store> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Store> jsonDeserializer = new JsonDeserializer<>(Store.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    }
}
