package com.aamend.hadoop.MapReduce;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class sparkTest1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf); 
		JavaRDD<String> lines = sc.textFile("data/textFile.txt");
		
		System.out.println("The number of lines of the file is: " + lines.count());
		
		@SuppressWarnings("serial")
		JavaRDD<String> sparkLines = lines.filter(new Function<String,Boolean>() {
			public Boolean call(String line){ return line.contains("Spark"); }
		}
				);
		
		System.out.println("The number of lines with the word Spark is: " + sparkLines.count());
 

	}

}
