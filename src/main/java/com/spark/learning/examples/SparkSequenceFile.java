package com.spark.learning.examples;


import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkSequenceFile {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

	    // Create a Java Spark Context. 
	    SparkConf conf = new SparkConf().setAppName("Spark Sequence File");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// get arguments from the input	
		String input = args[0];
		String output = args[1];
		
		JavaRDD<String> inputTextFile = sc.textFile(input);
		
		final Random generator = new Random(); 
		
		// read a text file, convert it as a pair RDD, save it as a sequence file, read it and finally display it. 
		JavaPairRDD<IntWritable, Text> keyValueRDD = inputTextFile.mapToPair(new PairFunction<String, IntWritable, Text>() {
			public Tuple2<IntWritable, Text> call(String string) {return new Tuple2<IntWritable, Text>(new IntWritable(generator.nextInt(100)),new Text(string));}
		});
		
		keyValueRDD.saveAsHadoopFile(output, IntWritable.class, Text.class, SequenceFileOutputFormat.class);
		
		JavaPairRDD<IntWritable, Text> readRDD = sc.sequenceFile(output, IntWritable.class, Text.class);
		JavaPairRDD<Integer, String> nativeRDD = readRDD.mapToPair(new PairFunction<Tuple2<IntWritable, Text>, Integer, String>() {
     		@Override
			public Tuple2<Integer, String> call(Tuple2<IntWritable, Text> tuple) {return new Tuple2<Integer, String>(tuple._1.get(),tuple._2.toString());}
		});
		
		displayPairRDD(nativeRDD,"Sequence file read from disk: ");

	}
	
	private static void displayPairRDD(JavaPairRDD<Integer, String> RDD, String message ) {

		System.out.println(message);
		
		for (Tuple2<Integer, String> tuple: RDD.take((int)RDD.count())) {
			System.out.println("Number: " + tuple._1 + " - Text: " + tuple._2);
		}

		System.out.println();
	}

}
