package edu.ismagi.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {

        // Créer une configuration Hadoop
        Configuration conf = new Configuration();

        // Créer un Job Hadoop nommé "word count"
        Job job = Job.getInstance(conf, "word count");

        // Spécifier la classe principale
        job.setJarByClass(WordCount.class);

        // Spécifier la classe Mapper
        job.setMapperClass(TokenizerMapper.class);

        // Spécifier la classe Combiner (réduction locale)
        job.setCombinerClass(IntSumReducer.class);

        // Spécifier la classe Reducer
        job.setReducerClass(IntSumReducer.class);

        // Types des clés et valeurs de sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Chemin du fichier d'entrée (args[0])
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Chemin du dossier de sortie (args[1])
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exécuter le job et sortir proprement
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
