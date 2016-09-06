package spark;

import org.apache.spark.launcher.SparkLauncher;

public class MyLauncher {
	public static void main(String[] args) throws Exception {
		Process process = new SparkLauncher()
				// spark home
				.setSparkHome("/Users/zhoum/Documents/tools/spark-2.0.0-bin-hadoop2.7")
				// spark application jar's directory
				.setAppResource("/Users/zhoum/Documents/workspace-git/spark-example/target/spark-example-0.0.1-SNAPSHOT.jar")
				// spark application's main class
				.setMainClass("spark.CountChars")
				// spark master's URL of the spark cluster 
				.setMaster("spark://selden.niwa.co.nz:7077")
				// launch spark application
				.launch();
		
		// wait until subprocesses finished
		process.waitFor();
	}
}
