package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    static public Serde<Greeting> greetingSerdes() {
        return new GreetingSerdes();
    }

    static public Serde<Greeting> greetingSerdesUsingGenerics() {
        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Greeting> jsonDeserializer = new JsonDeserializer<>(Greeting.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
