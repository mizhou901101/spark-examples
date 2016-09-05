package spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * CountChars is a Spark application, which counts each char's appearing times in a file.
 * 
 * note: When setup your cluster, the docs recommend to have your number of partitions set to 3 - 4 times the number of CPUs in your cluster,
 * so that the work gets distributed more evenly among the CPUs. So 3-4 cores will be enough for 4 + 8 partitions.
 * 
 * @author zhoum
 * 
 */
public class CountChars {
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		// input file
		String logFile = "/tmp/wifi-gW8plj.log";
		SparkConf conf = new SparkConf().setAppName("Spark - Count chars");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/**
		 * The textFile method also takes an optional second argument for
		 * controlling the number of partitions of the file. By default, Spark
		 * creates one partition for each block of the file (blocks being 64MB
		 * by default in HDFS), but you can also ask for a higher number of
		 * partitions by passing a larger value. Note that you cannot have fewer
		 * partitions than blocks.
		 */
		JavaRDD<String> textFile = sc.textFile(logFile, 4); // 4 partitions in parallel
		
		// get a new JavaRDD by splitting texts in to single chars
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("")).iterator();
			}
		});
		
		// give each char 1 value
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		
		// reduce by key, accumulate values as appearing times in 8 partition in parallel
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		}, 8);
		
		// coalesce into a single partition to save the result
		counts.coalesce(1, true).saveAsTextFile("/tmp/spark-resut");

		sc.close();
	}
}
