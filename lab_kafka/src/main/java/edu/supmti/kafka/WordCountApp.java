package edu.supmti.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: WordCountApp <input-topic> <output-topic>");
            return;
        }

        String inputTopic = args[0];
        String outputTopic = args[1];

        // Configuration Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Construction du pipeline Streams
        StreamsBuilder builder = new StreamsBuilder();

        // Lire les lignes du topic d'entrée
        KStream<String, String> textLines = builder.stream(inputTopic);

        // WordCount : découpage + regroupement + comptage
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count();

        // Convertir le résultat en KStream et envoyer au topic de sortie
        wordCounts
                .toStream()
                .mapValues(count -> Long.toString(count))
                .to(outputTopic);

        // Lancer l'application Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("WordCountApp started. Reading from: " + inputTopic + ", writing to: " + outputTopic);

        // Arrêt propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
