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
				return split[2] + ";" + split[3];

				});
		//on sépare les patterns
		JavaPairRDD<String, Double> totalDurationPerPattern = notIdle.flatMapToPair(line -> 
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
		StorageLevel sl = new StorageLevel();
		totalDurationPerPattern = totalDurationPerPattern.persist(sl);
		
		
		JavaDoubleRDD aPatternDuration = totalDurationPerPattern.mapToDouble(tuple -> 
		{
			return tuple._2;
		});

		//Pour récuperer la somme total afin de calculer les pourcentage plus tard.
		Double totalSumOfDuration = aPatternDuration.stats().sum();

		JavaPairRDD<String, Double> durationPercentByPattern = totalDurationPerPattern.mapToPair(tuple -> {
			return new Tuple2<String,Double>(tuple._1, (Double)(tuple._2 * 100.0D / totalSumOfDuration ));
		} );
		
		//Que 22 motifs donc collect pas méchant mais sinon on ecrirait directement avec un saveastextfile
		List<Tuple2<String,Double>> durationPercentByPatternList = durationPercentByPattern.collect();
		//durationPercentByPattern.saveAsTextFile("EXO6/durationPercent/");
		try {
			writeToLocal(context, "Exo6/patternPercentDuree.txt", durationPercentByPatternList.toString());
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}

		// EXERCICE B : Top 10 pattern en représentativité.
		List<Tuple2<String,Double>> topNPatternPercentDuration = durationPercentByPattern.top(topN,SerializableComparator.serialize((v, w) -> {return Double.compare(v._2, w._2); } ) );
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
		System.out.println(durationPercentByPatternList.toString());
		System.out.println("######  EXO 6 : B ######");
		System.out.println("Top 10 pattern : ");
		System.out.println(topNPatternPercentDuration.toString());

		context.close();
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
