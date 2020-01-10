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

public class Exo4 {

	//EXERCICE 4
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Exo4");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> distFile = context.textFile(args[0]);

		// Le nombre de job dans le top
		int topN = 10;

		// Le nombre de tranches de notre histograme, doit etre un entier strictement positif.
		int nTranches = 5;
		try {
			if(args.length == 2 && args[1] != null && Integer.parseInt(args[1]) > 0) {
				nTranches = Integer.parseInt(args[1]);
				System.err.println(Integer.parseInt(args[1]));
			}
		} catch(Exception ex) {
			System.err.println("#######################################################################################################");
			System.err.println("Erreur : Le 2nd paramétre doit être un nombre entier positif pour le nombre de tranches des histogrames");
			System.err.println("#######################################################################################################");
			System.exit(0); 
		}

		// EXERCICE 4 : La distribution de duréee par job.
		JavaRDD<String> notIdle = distFile.filter(activity -> 
		{
			String[] split = activity.split(";");
			return !split[0].equals("start") && !split[4].equals("0");
		} ).map(activity -> 
				{ 
				String[] split = activity.split(";");
				return split[2] + ";" + split[5];

				});
		//On sépare les lignes avec plusieur patterns pour le group by qui va suivre, augmentation du nombre de ligne
		JavaRDD<String> notIdleDurationByJob = notIdle.flatMap( activity -> {
			String[] split = activity.split(";");
			String[] jobs = split[1].split(",");
			ArrayList<String> al = new ArrayList<String>();
			for(String job : jobs)
			 {
				al.add(split[0]+";"+job);
			 }
			 return al.iterator();
		});
		
		//On groupe par pattern
		//JavaPairRDD<String, Iterable<String>> groupBySingleJob = notIdleDurationByJob.groupBy(activity -> activity.split(";")[1]);
		JavaPairRDD<String, Double> durationBySingleJob = notIdleDurationByJob.groupBy(activity -> activity.split(";")[1]).mapToPair( tuple -> {
			double sum =0;
			for(String duration : tuple._2)
			{
				sum+= Double.parseDouble(duration.split(";")[1]);
			}
			return new Tuple2<String,Double>(tuple._1, sum);
		});
		StorageLevel sl = new StorageLevel();
		durationBySingleJob = durationBySingleJob.persist(sl);

		// EXERCICE A : La distribution de duréee par job.
		JavaDoubleRDD aJobDuration = durationBySingleJob.mapToDouble(tuple -> tuple._2);
		aJobDuration = aJobDuration.persist(sl);

		//Pour récuperer l'histogramme.
		Tuple2<double[], long[]> aJobDurationHistogram = aJobDuration.histogram(nTranches);
		//Pour récuperer le minimum, le maximum et la moyenne.
		StatCounter aJobDurationStat = aJobDuration.stats();
		//Pour récuperer médiane, premier et troisième quadrants.
		double[] aJobDurationPercentiles = getPercentiles(aJobDuration.map(activity -> { return activity; }), new double[]{0.25, 0.5, 0.75}, aJobDuration.count(),  17);

		synthetyzeToFile(context, "Exo4/distriDureeByJob.txt", aJobDurationStat, aJobDurationPercentiles, aJobDurationHistogram, nTranches);

		// EXERCICE B : Top 10 jobs en temps total d’accès au PFS.
		List<Tuple2<String,Double>> topNJobDuration = durationBySingleJob.top(topN,((v, w) -> {return Double.compare(v._2, w._2); } ) );
		try {
			writeToLocal(context, "Exo4/topTenJobTotalDuree.txt", topNJobDuration.toString());
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}

		//Partie affichage des exo A et B
		System.out.println("######  EXO 4 : A ######");
		System.out.println("La distribution de duréee par job");
		showStat(aJobDurationStat);
		showQ1MQ3(aJobDurationPercentiles[0], aJobDurationPercentiles[1], aJobDurationPercentiles[2]);
		showHistogram(aJobDurationHistogram, nTranches);

/*		System.out.println("######  EXO 4 : B ######");
		System.out.println("Top 10 jobs en temps total d’accès au PFS. : ");
		System.out.println(topNJobDuration.toString());
*/
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

	/*
     * @brief Permet de créer et d'ecrire dans un fichier en HDFS
	 * @param sparkContext le Spark Context
	 * @param filename le nom du fichier avec son repertoire, par default il sera dans le repertoire de l'utilisateur
	 * @param statCounter qui contient les statistiques Spark collecteé sur un JavaRDD.
	 * @param percentiles la liste des percentiles calculés
	 * @param histogram qui contient les valeurs etudier sous la forme d'un Tuple2<double[], long[]>
	 * @param nTranches le nombre de tranches découper dans les données de l'histogramme
	*/
	private static void synthetyzeToFile(JavaSparkContext sparkContext, String filename, StatCounter statCounter, double[] percentiles, Tuple2<double[], long[]> histogram, int nTranches){

		StringBuilder header= new StringBuilder("minimum;maximum;moyenne;médiane;premier quadrants;troisième quadrants");
		StringBuilder value = new StringBuilder(statCounter.min() + ";" + statCounter.max() + ";" + statCounter.mean() + ";" + percentiles[1] + ";" + percentiles[0] + ";" + percentiles[2]);

		if (histogram != null) {
			for (int i = 0; i < nTranches; ++i) {
				header.append(";hist T" + (i+1));
				value.append(";" + histogram._1()[i] + "," + histogram._2()[i]);
			}
		}
		header.append("\n");
		header.append(value);
		try {
			writeToLocal(sparkContext, filename, header.toString());
		} catch(IOException ex){
			System.err.println("############################################");
			System.err.println("Problème avec l'écriture sur fichier en HDFS");
			System.err.println("############################################");
		}
	}

	/*
     * @brief Permet d'afficher le min, max et la moyenne recupérer par un StatCounter.
	 * @param statCounter qui contient les statistiques Spark collecteé sur un JavaRDD.
	*/
	private static void showStat(StatCounter statCounter) {
		if (statCounter != null) {
			System.out.println("+-------------------------------+-------------------------+\n");
			System.out.println("| Min \t| Max \t\t\t| Mean \t\t|");
			System.out.println("+-------------------------------+-------------------------+\n");
			System.out.println("| " + statCounter.min() + " \t |\t " + statCounter.max() + "\t|\t " + statCounter.mean() + "\t|");
		}
	}

	/*
     * @brief Permet d'afficher la 1er quartant, la mediane et le 3eme quartant.
	 * @param q1
	 * @param qm
	 * @param q3
	*/
	private static void showQ1MQ3(double q1, double m, double q3) {
		System.out.println("+-----------------------------------------------+");
		System.out.println("|\t Q1 \t|\t Q2 \t|\t Q3 \t|");
		System.out.println("+-----------------------------------------------+");
		System.out.println("|\t " + q1 + " \t|\t " + m + " \t|\t " + q3 + " \t|");
		System.out.println("+-----------------------------------------------+\n");
	}

	/*
     * @brief Permet d'afficher les tranches d'un histogramme
	 * @param histogram qui contient les valeurs etudier sous la forme d'un Tuple2<double[], long[]>
	 * @param nTranches le nombre de tranches découper dans les données de l'histogramme
	*/
	private static void showHistogram(Tuple2<double[], long[]> histogram, int nTranches) {
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


	/*
     * @brief Permet de calculer les percentiles (D1, D9, Q1, Mediane, Q3, etc). Ex: D1 s'écrira 0.10, la mediane 0.50.
	 * @param rdd la liste des valeurs sous forme de double
	 * @param percentiles liste des percentiles que l'ont souhaite calculer, ex : 0.25 pour Q1 
	 * @param rddSize taille du JavaRDD<Double> rdd
	 * @param numPartitions le nombre de partition, necessaire pour la fonction sortBy() du JavaRDD<Double> rdd
	 * @return double[] la liste des percentiles calculés. Sa taille équivaut a celle du @param percentiles
	*/
	public static double[] getPercentiles(JavaRDD<Double> rdd, double[] percentiles, long rddSize, int numPartitions) {
		double[] values = new double[percentiles.length];
		JavaRDD<Double> sorted = rdd.sortBy((Double d) -> d, true, numPartitions);
		JavaPairRDD<Long, Double> indexed = sorted.zipWithIndex().mapToPair((Tuple2<Double, Long> t) -> t.swap());
	
		for (int i = 0; i < percentiles.length; i++) {
			double percentile = percentiles[i];
			long id = (long) (rddSize * percentile);
			values[i] = indexed.lookup(id).get(0);
		}
	
		return values;
	}

}
