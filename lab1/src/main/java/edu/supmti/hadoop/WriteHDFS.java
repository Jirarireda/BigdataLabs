package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {

        // Vérification des arguments
        if (args.length < 2) {
            System.out.println("Usage: WriteHDFS <chemin_fichier> <texte_à_écrire>");
            System.exit(1);
        }

        String chemin = args[0]; // Chemin du fichier sur HDFS
        String texte = args[1]; // Contenu à écrire

        // Configuration Hadoop
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(chemin);

        // Si le fichier existe déjà, on le supprime
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("Ancien fichier supprimé : " + chemin);
        }

        // Écriture sur HDFS
        FSDataOutputStream out = fs.create(path);
        out.writeUTF("=== Nouveau fichier écrit depuis WriteHDFS ===\n");
        out.writeUTF(texte + "\n");
        out.close();

        System.out.println("✅ Fichier écrit avec succès sur HDFS : " + chemin);

        fs.close();
    }
}
