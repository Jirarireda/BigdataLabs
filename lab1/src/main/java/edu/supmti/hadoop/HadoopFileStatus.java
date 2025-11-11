package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {

        // Vérification des paramètres
        if (args.length < 3) {
            System.out.println("Usage: HadoopFileStatus <chemin> <nom_fichier> <nouveau_nom>");
            System.exit(1);
        }

        String chemin = args[0];
        String nomFichier = args[1];
        String nouveauNom = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(chemin, nomFichier);
        if (!fs.exists(filePath)) {
            System.out.println("Fichier introuvable : " + filePath);
            System.exit(1);
        }

        // Informations sur le fichier
        FileStatus status = fs.getFileStatus(filePath);
        System.out.println("Nom du fichier : " + status.getPath().getName());
        System.out.println("Taille : " + status.getLen() + " octets");
        System.out.println("Propriétaire : " + status.getOwner());
        System.out.println("Permissions : " + status.getPermission());
        System.out.println("Bloc size : " + status.getBlockSize());
        System.out.println("Replication : " + status.getReplication());

        // Localisation des blocs
        BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation loc : locations) {
            System.out.println("Bloc offset : " + loc.getOffset());
            System.out.println("Bloc length : " + loc.getLength());
            System.out.print("Hôtes : ");
            for (String host : loc.getHosts()) {
                System.out.print(host + " ");
            }
            System.out.println();
        }

        // Renommage du fichier
        Path nouveauPath = new Path(chemin, nouveauNom);
        boolean result = fs.rename(filePath, nouveauPath);
        if (result)
            System.out.println("Fichier renommé en : " + nouveauNom);
        else
            System.out.println("Échec du renommage.");

        fs.close();
    }
}
