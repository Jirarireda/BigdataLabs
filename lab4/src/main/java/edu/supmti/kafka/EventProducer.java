package edu.supmti.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class EventProducer {

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Usage: java EventProducer <topic>");
            return;
        }

        String topicName = args[0];

        // ===== Kafka Configuration =====
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 10; i++) {

                String key = String.valueOf(i);
                String value = "Message " + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

                producer.send(record);

                System.out.println("Envoyé -> " + value);
            }
        } finally {
            producer.flush();
            producer.close();
        }

        System.out.println("Tous les messages envoyés avec succès !");
    }
}
