package edu.supmti.kafka;

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class WordProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Scanner sc = new Scanner(System.in);

        System.out.println("Tape des mots :");

        while (true) {
            String line = sc.nextLine();
            producer.send(new ProducerRecord<>("WordCount-Topic", line));
        }
    }
}
