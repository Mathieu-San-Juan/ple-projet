package bigdata;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;

//0- Start
//1- End
//2- Duration
//3- patterns
//4- npattern
//5- jobs
//6- njob
//7- days
//8- ndays

public class Exo7 {

	//EXERCICE 7
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Exo7");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> distFile = context.textFile(args[0]);

		String[] patterns = {"0","1","2","3"};

		if(args.length != 1 && args.length != 5) {
			System.err.println("#############################################################################################################");
			System.err.println("Erreur : Le nombre d'argument est erroné. En permier le chemin (déjà rempli), puis soit 4 pattern soit rien.");
			System.err.println("#############################################################################################################");
			System.exit(0); 
		}

		try {
			if(args.length == 5 && args[1] != null && Integer.parseInt(args[1]) >= 0
					&& args[2] != null && Integer.parseInt(args[2]) >= 0
					&& args[3] != null && Integer.parseInt(args[3]) >= 0
					&& args[4] != null && Integer.parseInt(args[4]) >= 0) {
				patterns[0] = args[1];
				patterns[1] = args[2];
				patterns[2] = args[3];
				patterns[3] = args[4];
			}
		} catch(Exception ex) {
			System.err.println("#################################################################################################################");
			System.err.println("Erreur : Le 2eme, 3eme, 4eme et 5eme paramétres doient être des nombres entier positif pour le 4 pattern étudiés.");
			System.err.println("#################################################################################################################");
			System.exit(0); 
		}

		if(patterns[0] == patterns[1] || patterns[0] == patterns[2] || patterns[0] == patterns[3]
				|| patterns[1] == patterns[2] || patterns[1] == patterns[3] || patterns[2] == patterns[3]) 
		{
			System.err.println("#################################################################################################################");
			System.err.println("Erreur : Les 4 pattern étudiés doivent être différent, aucun doublons accepté.");
			System.err.println("#################################################################################################################");
			System.exit(0); 
		}


		// EXERCICE 7 : Proposez et implémentez une solution permettant d’obtenir toutes les plages horaires comportant 4 patterns donnés.
		JavaRDD<String> plageHoraireFilterByPatternList = distFile.filter(
				activity -> 
				{
					String[] split = activity.split(";");
					return (!split[0].equals("start") && Integer.valueOf(split[4])>=patterns.length) && Arrays.asList(split[3].split(",")).containsAll(Arrays.asList(patterns));
				}
			).map(
				activity -> { 
					String[] split= activity.split(";");
					return split[0] + ";" + split[1] + ";" + split[3];
				});

		plageHoraireFilterByPatternList.saveAsTextFile("Exo7/plageHoraireByPattern");

		context.close();
	}
}
