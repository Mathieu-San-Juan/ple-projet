package bigdata;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;

//0- Start
//1- End
//2- Duration
//3- patterns
//4- npattern
//5- jobs
//6- njob
//7- days
//8- ndays

public class Exo6 {

	//EXERCICE 6
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Exo6");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> distFile = context.textFile(args[0]);

		int topN = 10;

		// EXERCICE A : Pourcentage du temps total de phases où chaque motif a été observé seul (qu’un seul motif pendant la phase), ou concurrent à des autres.
		JavaRDD<String> notIdle = distFile.filter(activity -> 
			{
				String[] split = activity.split(";");
				return !split[0].equals("start") && !split[4].equals("0");
			} ).map(v -> 
				{ 
				String[] split = v.split(";");
				return split[2] + ";" + split[3];

				});

		JavaRDD<String> notIdleDurationByPattern = notIdle.flatMap((v)-> {
			String[] split = v.split(";");
			String[] patterns = split[1].split(",");
			ArrayList<String> al = new ArrayList<String>();
			for(String pattern : patterns)
			 {
				al.add(split[0]+";"+pattern);
			 }
			 return al.iterator();
		});

		JavaPairRDD<String, Iterable<String>> groupBySinglePattern = notIdleDurationByPattern.groupBy(activity -> activity.split(";")[1]);
		
		JavaDoubleRDD aPatternDuration = groupBySinglePattern.mapToDouble(tuple -> 
		{
			double sum =0;
			for(String duration : tuple._2)
			{
				sum+= Double.parseDouble(duration.split(";")[1]);
			}
			return sum;
		});

		//Pour récuperer la somme total afin de calculer les pourcentage plus tard.
		Double totalSumOfDuration = aPatternDuration.stats().sum();

		JavaPairRDD<String, Double> durationPercentByPattern = groupBySinglePattern.mapToPair( tuple -> {
			double sum =0;
			for(String duration : tuple._2)
			{
				sum+= Double.parseDouble(duration.split(";")[1]);
			}
			return new Tuple2<String,Double>(tuple._1, (Double)(sum / totalSumOfDuration));
		});
		StorageLevel sl = new StorageLevel();
		durationPercentByPattern = durationPercentByPattern.persist(sl);
		
		try {	
			writeToLocal(context, "Exo6/durationPercentByPattern.txt", durationPercentByPattern.toString());
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}

		// EXERCICE B : Top 10 pattern en représentativité.
		List<Tuple2<String,Double>> topNPatternPercentDuration = durationPercentByPattern.top(topN,((v, w) -> {return Double.compare(v._2, w._2); } ) );
		try {
			writeToLocal(context, "Exo6/topTenPatternPercentDuree.txt", topNPatternPercentDuration.toString());
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}

		//Partie affichage des exo A et B
		System.out.println("######  EXO 6 : A ######");
		System.out.println("Pourcentage du temps total de phases où chaque motif a été observé seul ou concurrent");
		System.out.println(durationPercentByPattern.toString());
		System.out.println("######  EXO 6 : B ######");
		System.out.println("Top 10 pattern : ");
		System.out.println(topNPatternPercentDuration.toString());

		context.close();
	}

	/*
     * @brief Permet de créer et d'ecrire dans un fichier en HDFS
	 * @param sparkContext le Spark Context
	 * @param filename le nom du fichier avec son repertoire, par default il sera dans le repertoire de l'utilisateur
	 * @param text le contenue que l'on souhaite écrire dans le fichier 
	*/
	private static void writeToLocal(JavaSparkContext sparkContext, String filename, String text) throws IOException {
		FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration()); 
		FSDataOutputStream output = fs.create(new Path(filename));
		// But BufferedOutputStream must be used to output an actual text file.
		BufferedOutputStream os = new BufferedOutputStream(output);
		os.write(text.getBytes("UTF-8"));
		os.close();
	}


}
