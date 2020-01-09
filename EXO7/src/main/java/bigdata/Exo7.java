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

	//EXERCICE 1
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Exo7");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> distFile = context.textFile(args[0]);

		String[] patterns = {"0","1","5","6"};

		// EXERCICE 7 : Proposez et implémentez une solution permettant d’obtenir toutes les plages horaires comportant 4 patterns donnés.
		JavaRDD<String> plageHoraireFilterByPatternList = distFile.filter(
				activity -> (!activity.split(";")[0].equals("start") && !activity.split(";")[6].equals("0") && Integer.valueOf( activity.split(";")[4])>=patterns.length )
			).filter(v -> 
				{
					return Arrays.asList(v.split(";")[3].split(",")).containsAll(Arrays.asList(patterns));
				}
				).map(v -> { 
					String[] split= v.split(";");
					return split[0] + ";" + split[1];
				});
		
		plageHoraireFilterByPatternList.saveAsTextFile("Exo7/plageHoraireFilterByPatternList.txt");

	
		context.close();
	}
}
