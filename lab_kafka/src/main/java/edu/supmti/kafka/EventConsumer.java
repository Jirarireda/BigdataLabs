package edu.supmti.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class EventConsumer {

    public static void main(String[] args) throws Exception {

        // Vérifier le nom du topic
        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0].toString();

        // Propriétés du consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // S'abonner au topic
        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("Consommation des messages du topic : " + topicName);

        // Lire les messages en boucle
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(
                        "offset = " + record.offset() +
                                ", key = " + record.key() +
                                ", value = " + record.value());
            }
        }
    }
}
