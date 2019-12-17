package bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.codahale.metrics.Histogram;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;

public class projet {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> distFile = context.textFile(args[0]);
		//On vérifie que la ligne est Idle (Colonne 3 List des patterns = -1)
		Function<String, Boolean> isIdle = k -> 
		{
			String splitTab[] = k.split(";");
		 	return splitTab[4].equals("0");
		};
		//On vérifie que la ligne n'est =as Idle (Colonne 3 List des patterns != -1) et on 
		//ne prends pas la première ligne (d'où le !splitTab[3].equals("phases") )
		Function<String, Boolean> isNotIdle = k -> 
		{
			String splitTab[] = k.split(";");
		 	return !(splitTab[4].equals("0") || splitTab[4].equals("nphases") );
		};
		JavaRDD<String> withIdle = distFile.filter(isIdle);
		//On converti en double pour calculer la variance
		//JavaDoubleRDD withoutIdleDuration = withoutIdle.mapToDouble(S -> Double.valueOf(S.split(";")[2]) );
		JavaDoubleRDD withIdleDuration = distFile.filter(isIdle).mapToDouble(S -> Double.valueOf(S.split(";")[2]) );
		JavaPairRDD<String, String> pair = withIdle.mapToPair(s ->  {
			String split = s.split(";")[3];
			return new Tuple2<String,String>(split,s);
		});
		JavaPairRDD<String, Iterable<String>> groupByKeyPattern = pair.groupByKey();
		//JavaPairRDD<String, Iterable<String>> groupByPattern = withIdle.groupBy(row -> row.split(";")[4]);
		System.out.println("patate");
		//groupByKeyPattern.foreach(s -> System.out.println(s._1()));

		System.out.println(groupByKeyPattern.count());
		System.out.println("patate");
		groupByKeyPattern.foreach(s -> System.out.println(s._1()));
		//On Calcule la variance avec les methodes de spark
		//System.out.println(withoutIdleDuration.variance());
		//System.out.println(withIdleDuration.variance());


		context.close();
		
		//cachedRatingsForUser.unpersist(false);
	}
	
}
