package edu.supmti.kafka;

import java.util.*;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;

public class WordCountConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "wordcount");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("WordCount-Topic"));

        Map<String, Integer> counts = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> r : records) {
                String[] words = r.value().split(" ");
                for (String w : words) {
                    counts.put(w, counts.getOrDefault(w, 0) + 1);
                }
            }

            System.out.println(counts);
        }
    }
}
