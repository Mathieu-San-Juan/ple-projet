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
		JavaRDD<String> distFile = context.textFile(args[0]).filter(line -> !line.split(";")[0].equals("start") );
		
		int nTranches = 10;
		//Exo 1 Pour les phases qui ne sont pas idle, la distribution de leur durée	
		JavaDoubleRDD doubleDurationActivitiesNotIdle = distFile
			.filter(activity -> Integer.parseInt(activity.split(";")[4]) > 0)
			.mapToDouble(activity -> Double.parseDouble(activity.split(";")[2]) );
		
		Tuple2<double[],long[]> histogramNotIdle = doubleDurationActivitiesNotIdle.histogram(nTranches);
		StatCounter statNotIdle = doubleDurationActivitiesNotIdle.stats();
		
		//Exo 2 Pour les phases qui sont idle, la distribution de leur durée
		JavaDoubleRDD doubleDurationActivitiesIdle = distFile
			.filter(activity -> Integer.parseInt(activity.split(";")[4]) > 0)
			.mapToDouble(activity -> Double.parseDouble(activity.split(";")[2]) );
		
		Tuple2<double[],long[]> histogramIdle = doubleDurationActivitiesIdle.histogram(nTranches);
		StatCounter statIdle = doubleDurationActivitiesIdle.stats();

		showDistribution("Exo 1 duration for not IDLE", statNotIdle, histogramNotIdle, nTranches);
		showDistribution("Exo 2 duration for IDLE", statIdle, histogramIdle, nTranches);

		//cachedActivitiesNotIdle.unpersist(false);
		
		context.close();
	}

	private static void showDistribution(String title, StatCounter statCounter, Tuple2<double[],long[]> histogram, int nTranches) {
		System.out.println(title);
		System.out.println("+---------------------------------+");
		System.out.println("|\t Min \t|\t\t Max \t\t|\t\t Mean \t\t|");
		System.out.println("+---------------------------------+");
		System.out.println("|\t " + statCounter.min() + " \t|\t " + statCounter.max() + " \t|\t " + statCounter.mean() + " \t|");
		System.out.println("+-------------------+---------------------------------+\n\n");

		System.out.println("Histogram sur " + nTranches + " tranche(s).");
		System.out.println("+------------------------+------------------------+");
		System.out.println("|         Valeur         |        Nombre          |");
		for(int i=0; i< nTranches; ++i)
		{
			System.out.println("| " + histogram._1()[i] + " \t | " + histogram._2()[i] + "\t\t|");
			System.out.println("+------------------------+------------------------+");
		}
	}
	
}
