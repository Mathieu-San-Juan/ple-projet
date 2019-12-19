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
		JavaRDD<String> distFile = context.textFile(args[0]);
		
		//Exo 1 Pour les phases qui ne sont pas idle, la distribution de leur durée	
		JavaDoubleRDD doubleDurationActivitiesNotIdle = distFile
			.filter(line -> !line.split(";")[0].equals("start") )
			.filter(activity -> Integer.parseInt(activity.split(";")[4]) > 0)
			.mapToDouble(activity -> Double.parseDouble(activity.split(";")[2]) );
		
		StatCounter statDurationActivitiesNotIdle = doubleDurationActivitiesNotIdle.stats();
		int nTranche = 10;
		Tuple2<double[],long[]> test=doubleDurationActivitiesNotIdle.histogram(nTranche);
		double[] d =test._1 ;
		long[] l =test._2 ;
		System.out.println("+------------------------+------------------------+");
		System.out.println("|         Valeur         |        Nombre          |");
		for(int i=0; i< nTranche; ++i)
		{
			System.out.println("| " + d[i] + " \t | " + l[i] + "\t\t|");
			System.out.println("+------------------------+------------------------+");
		}

		double minNotIdle = statDurationActivitiesNotIdle.min();
		double maxNotIdle = statDurationActivitiesNotIdle.max();
		double meanNotIdle = statDurationActivitiesNotIdle.mean();
		
		/*//Exo 2 Pour les phases qui sont idle, la distribution de leur durée
		JavaDoubleRDD doubleDurationActivitiesIdle = distFile
			.filter(line -> !line.split(";")[0].equals("start") )
			.filter(activity -> Integer.parseInt(activity.split(";")[4]) == 0)
			.mapToDouble(activity -> Double.parseDouble(activity.split(";")[2]) );

		StatCounter statDurationActivitiesIdle = doubleDurationActivitiesIdle.stats();

		double minIdle = statDurationActivitiesIdle.min();
		double maxIdle = statDurationActivitiesIdle.max();
		double meanIdle = statDurationActivitiesIdle.mean();

*/
		//cachedActivitiesNotIdle.unpersist(false);
		System.out.println("+-------------------+---------------------------------+");
		System.out.println("|Pour les activités |\t Min \t|\t\t Max \t\t|\t\t Mean \t\t|");
		System.out.println("--------------------+----------------------------------");
		System.out.println("|     NotIdle       |\t " + minNotIdle + " \t|\t " + maxNotIdle + " \t|\t " + meanNotIdle + " \t|");
		System.out.println("+-------------------+---------------------------------+");
		//System.out.println("|      Idle         |\t " + minIdle + " \t|\t " + maxIdle + " \t|\t " + meanIdle + " \t|");*/
		context.close();
	}
	
}
