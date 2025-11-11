package edu.supmti.hadoop;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {

        // Vérification des paramètres
        if (args.length < 1) {
            System.out.println("Usage: ReadHDFS <chemin_fichier>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(args[0]);
        if (!fs.exists(filePath)) {
            System.out.println("Fichier introuvable : " + filePath);
            System.exit(1);
        }

        // Lecture du contenu
        FSDataInputStream inputStream = fs.open(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String line;
        System.out.println("=== Contenu du fichier " + filePath.getName() + " ===");
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        reader.close();
        fs.close();
    }
}
