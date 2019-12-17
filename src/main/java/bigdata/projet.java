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
		
		JavaRDD<Activity> activities = context.textFile(args[0])
		.filter(line -> !line.split(";")[0].equals("start") )
        .map(line -> line.split(";"))
        .map(row -> new Activity(
                Long.parseLong(row[0]),
                Long.parseLong(row[1]),
                Long.parseLong(row[2]),
				row[3].split(","),
				Integer.parseInt(row[4]),
				row[5].split(","),
				Integer.parseInt(row[6]),
				row[7].split(","),
				Integer.parseInt(row[8])
		));
		//Exo 1 Pour les phases qui ne sont pas idle, la distribution de leur durée
		JavaRDD<Activity> cachedActivitiesNotIdle = activities
			.filter(activity -> activity.nPatterns > 0)
			.cache();

		JavaDoubleRDD doubleDurationActivitiesNotIdle = cachedActivitiesNotIdle
		.mapToDouble(activity -> activity.duration );


		JavaRDD<Activity> distData = cachedActivitiesNotIdle.parallelize();
		cachedActivitiesNotIdle.
		

		//int nStep = 10;
		//double step = (maxNotIdle - minNotIdle) / (double)(nStep);

		
		//Exo 2 La distribution de la durée des phases idle.
		//JavaRDD<Activity> cachedActivitiesIdle = activities
		//	.filter(activity -> activity.nPatterns == 0)
		//	.cache();
		
		//double meanIdle = cachedActivitiesIdle
		//	.mapToDouble(activity -> activity.duration )
		//	.mean();

		//cachedActivitiesNotIdle.unpersist(false);
		//System.out.println("|Pour les activités |\t Min \t|\t\t Max \t\t|");
		//System.out.println("|     NotIdle       |\t " + minNotIdle + " \t|\t " + maxNotIdle + " \t|");
		//System.out.println("|      Idle         |\t" + meandle + "\t|");
		context.close();
	}
	
}
