package bigdata;

import java.io.BufferedOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

//0- Start
//1- End
//2- Duration
//3- patterns
//4- npattern
//5- jobs
//6- njob
//7- days
//8- ndays

public class Exo5 {

	//EXERCICE 5
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Exo5");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> distFile = context.textFile(args[0]);

		// EXERCICE 5 : La somme des durée de phases idle 
		JavaRDD<String> idle = distFile.filter(activity -> 
		{
			String[] split = activity.split(";");
			return !split[0].equals("start") && split[4].equals("0");
		} );

		JavaDoubleRDD durationIdle = idle.mapToDouble(activity -> Double.parseDouble(activity.split(";")[2]));
		Double totalDureeIdle = durationIdle.stats().sum();
		try {
			writeToLocal(context, "Exo5/totalDureeIdle.txt", String.valueOf(totalDureeIdle));
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}

		//Partie affichage
		System.out.println("######  EXO 5  ######");
		System.out.println("La somme des durée de phases idle : " + String.valueOf(totalDureeIdle));

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
