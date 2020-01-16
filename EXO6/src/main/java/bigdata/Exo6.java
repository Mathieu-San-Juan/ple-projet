package bigdata;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
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
		context.setLogLevel("WARN");
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
				return split[2] + ";" + split[3] + ";" + split[4];

				});
		StorageLevel sl = new StorageLevel();
		notIdle = notIdle.persist(sl);
		
		//D'abords les patterns qui arrivent seul
		JavaPairRDD<String, Double> totalDurationPerPatternSingle = notIdle.filter(activity -> 
			{
				return activity.split(";")[2].equals("1");
			} 
		).mapToPair(activity -> 
			{
				String[] split = activity.split(";");
				return new Tuple2<String,Double>(split[1], Double.parseDouble(split[0]));
			} 
		).reduceByKey((c1, c2) -> c1 + c2);
		totalDurationPerPatternSingle = totalDurationPerPatternSingle.persist(sl);
		
		//Puis les patterns qui arrivent en groupe, on sépare les patterns
		JavaPairRDD<String, Double> totalDurationPerPatternGrouped = notIdle.flatMapToPair(line -> 
			{
				String[] split = line.split(";");
				String[] patterns = split[1].split(",");
				ArrayList<Tuple2<String,Double>> al = new ArrayList<Tuple2<String,Double>>();
				for(String pattern : patterns)
				 {
					al.add(new Tuple2<String,Double>(pattern, Double.parseDouble(split[0])));
				 }
				 return al.iterator();
			 }
		).reduceByKey((c1, c2) -> c1 + c2);
		notIdle.unpersist();
		totalDurationPerPatternGrouped = totalDurationPerPatternGrouped.persist(sl);
		
		
		String distriSinglepattern = distribution(totalDurationPerPatternSingle);
		String toptenSinglepattern = topTen(totalDurationPerPatternSingle, topN);
		totalDurationPerPatternSingle.unpersist();
		
		String distriGroupedpattern = distribution(totalDurationPerPatternGrouped);
		String toptenGroupedpattern = topTen(totalDurationPerPatternGrouped, topN);
		totalDurationPerPatternGrouped.unpersist();
		
		
		// EXERCICE A : Les 22 patterns avec leurs temps total de duree en pourcentage
		//durationPercentByPattern.saveAsTextFile("EXO6/durationPercent/");
		try {
			writeToLocal(context, "Exo6/singlePatternPercentDuree.txt", distriSinglepattern);
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}
		try {
			writeToLocal(context, "Exo6/groupedPatternPercentDuree.txt", distriGroupedpattern);
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}

		// EXERCICE B : Top 10 pattern en représentativité.
		try {
			writeToLocal(context, "Exo6/topTenSinglePatternPercentDuree.txt", toptenSinglepattern);
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}
		try {
			writeToLocal(context, "Exo6/topTenGroupedPatternPercentDuree.txt", toptenGroupedpattern);
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}

		
		//Partie affichage des exo A et B
		System.out.println("######  EXO 6 : A ######");
		System.out.println("Pourcentage du temps total de phases où chaque motif a été observé seul");
		System.out.println(distriSinglepattern);
		System.out.println("Pourcentage du temps total de phases où chaque motif a été observé concurrent");
		System.out.println(distriGroupedpattern);
		System.out.println("######  EXO 6 : B ######");
		System.out.println("Top 10 pattern seul : ");
		System.out.println(toptenSinglepattern);
		System.out.println("Top 10 pattern concurrent : ");
		System.out.println(toptenGroupedpattern);

		context.close();
	}
	
	/*
     * @brief Permet de recuperer les total des durer en pourcentage pour les 22 motifs
	 * @param totalDurationPerPattern contenant en key l'id du pattern et en value le total
	*/
	public static String distribution(JavaPairRDD<String, Double> totalDurationPerPattern) {
		JavaDoubleRDD aPatternDuration = totalDurationPerPattern.mapToDouble(tuple -> 
		{
			return tuple._2;
		});

		//Pour récuperer la somme total afin de calculer les pourcentage plus tard.
		Double totalSumOfDuration = aPatternDuration.stats().sum();

		JavaPairRDD<String, Double> durationPercentByPattern = totalDurationPerPattern.mapToPair(tuple -> {
			return new Tuple2<String,Double>(tuple._1, (Double)(tuple._2 * 100.0D / totalSumOfDuration ));
		} );
		return durationPercentByPattern.collect().toString();
	}
	
	/*
     * @brief Permet de créer et d'ecrire dans un fichier en HDFS
	 * @param totalDurationPerPattern contenant en key l'id du pattern et en value le total
	 * @param topN le top que l'on souhaite
	*/
	public static String topTen(JavaPairRDD<String, Double> totalDurationPerPattern, int topN) {
		List<Tuple2<String,Double>> topNPatternPercentDuration = totalDurationPerPattern.top(topN,SerializableComparator.serialize((v, w) -> {return Double.compare(v._2, w._2); } ) );
		return topNPatternPercentDuration.toString();
	}
	
	public interface SerializableComparator<T> extends Comparator<T>, Serializable {

	  static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
	    return comparator;
	  }

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
