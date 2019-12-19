package bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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

//0- Start
//1- End
//2- Duration
//3- patterns
//4- npattern
//5- jobs
//6- njob
//7- days
//8- ndays

public class projet {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> distFile = context.textFile(args[0]);

		// Le nombre de tranches de notre histograme
		int nTranches = 5;

		// EXERCICE 1
		JavaRDD<String> notIdle = distFile.filter(activity -> (!activity.split(";")[0].equals("start") && !activity.split(";")[4].equals("0") ) );
		StorageLevel sl = new StorageLevel();
		notIdle.persist(sl);
		// Exo a Pour les phases qui ne sont pas idle, la distribution de leur durée
		JavaDoubleRDD durationNotIdle = notIdle
				.mapToDouble(activity -> Double.parseDouble(activity.split(";")[2]));
		durationNotIdle.persist(sl);

		Tuple2<double[], long[]> histogramNotIdle = durationNotIdle.histogram(nTranches);
//		StatCounter statNotIdle = durationNotIdle.stats();

		// Exo b Pour les phases qui sont idle, la distribution de leur durée
/*		JavaDoubleRDD durationIdle = distFile.filter(activity -> activity.split(";")[4].equals("0") )
				.mapToDouble(activity -> Double.parseDouble(activity.split(";")[2])).cache();

		Tuple2<double[], long[]> histogramIdle = durationIdle.histogram(nTranches);
		StatCounter statIdle = durationIdle.stats();
*/
		// Exo c Pour les phases qui on un seul mottif d'acces, la distribution de leur
		// durée, on utilise le notIdle puique le nPattern doit etre à 1, cela nous fait gagner du temps de lecture
	/*	JavaPairRDD<String, Iterable<String>> groupBySinglePattern = notIdle
				.filter(activity -> activity.split(";")[4].equals("1")).map(f -> {
					String[] row = f.split(";");
					return row[3] + ";" + row[2];
				}).groupBy(activity -> activity.split(";")[0]);
*/
		// HashMap<String, Tuple2<double[],long[]>> histogramForPhase = new
		// HashMap<String, Tuple2<double[],long[]>>();
		//List<Tuple2<String, Iterable<String>>> list = groupBySinglePattern.collect();
		//JavaPairRDD<String, Iterable<String>>[]

		/*
		 * groupBySinglePattern.foreach(f -> { List<String> result = new
		 * ArrayList<String>(); f._2.forEach(result::add); Tuple2<double[],long[]>
		 * histoForAPattern=context.parallelize(result).mapToDouble(duration ->
		 * Double.parseDouble(duration)).histogram(nTranches); });
		 */
		
		//long notIdleC = notIdle.count();
		//long idleC = durationIdle.count();
		//System.out.println("Exo 1.a duration for not IDLE " + notIdleC);
		//showStat(statNotIdle);
		//showDistribution(histogramNotIdle, nTranches);
		//System.out.println("Exo 1.b duration for IDLE "  + idleC);
		//showStat(statIdle);
		//showDistribution(histogramIdle, nTranches);

		// cachedActivitiesNotIdle.unpersist(false);

		context.close();
	}

	private static void showStat(StatCounter statCounter) {
		if (statCounter != null) {
			System.out.println("+-------------------------------+-------------------------+\n\n");
			System.out.println("| Min \t| Max \t\t\t| Mean \t\t\t|");
			System.out.println("+-------------------------------+-------------------------+\n\n");
			System.out.println("| " + statCounter.min() + " \t| " + statCounter.max() + " \t| "
					+ statCounter.mean() + " \t|");
					System.out.println("+-------------------------------+-------------------------+\n\n");
		}
	}

	private static void showDistribution(Tuple2<double[], long[]> histogram, int nTranches) {
		if (histogram != null) {
			System.out.println("Histogram sur " + nTranches + " tranche(s).");
			System.out.println("+------------------------+------------------------+");
			System.out.println("|\t Valeur  \t|\tNombre \t|");
			for (int i = 0; i < nTranches; ++i) {
				System.out.println("| " + histogram._1()[i] + " \t |\t " + histogram._2()[i] + "\t|");
				System.out.println("+------------------------+--------------+");
			}
		}
	}

}
