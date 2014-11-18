package com.spark.learning.examples;

import java.util.Arrays;
import java.util.Random;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SparkGrouping {
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Spark Grouping");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("data/textFile.txt");
		
		// create an RDD of words
		@SuppressWarnings("serial")
		JavaRDD<String> flatInput = input.flatMap(new FlatMapFunction<String,String>() {
			public Iterable<String> call(String string) { return Arrays.asList(string.split(" ")); }
			}
		);
		
		final Random generator = new Random(0);
		 
		// create a pair RDD with random keys for testing join() function
		@SuppressWarnings("serial")
		JavaPairRDD<Integer, String> pairRDD1 = flatInput.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String string) { return new Tuple2<Integer, String>(generator.nextInt(500),string);} // Notice the random number just for testing purposes
		});
		
		@SuppressWarnings("serial")
		JavaPairRDD<Integer, String> pairRDD2 = flatInput.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String string) { return new Tuple2<Integer, String>(generator.nextInt(500),string);} // Notice the random number just for testing purposes
		});
		
		JavaPairRDD<Integer,Tuple2<String, String>> innerJoin = pairRDD1.join(pairRDD2);
		JavaPairRDD<Integer,Tuple2<String, Optional<String>>> leftOuterJoin = pairRDD1.leftOuterJoin(pairRDD2);
		
		System.out.println("These are the first 10 elements of the join. Total length:" + innerJoin.count());
		
		for (Tuple2<Integer,Tuple2<String, String>> tuple: innerJoin.take(10)) {
			 System.out.println("(" + tuple._1 + ", (" + tuple._2._1 + ", " + tuple._2._2 + "))");
		};
		
		System.out.println("These are the first 20 elements of the leftOuterJoin. Total length:" + leftOuterJoin.count());
		
		for (Tuple2<Integer,Tuple2<String, Optional<String>>> tuple: leftOuterJoin.take(20)) {
			 System.out.println("(" + tuple._1 + ", <" + tuple._2._1 + " - " + tuple._2._2.get() + ">)");
		};
		
		
		System.out.println("Sort JavaPairRDD in descendent order and display its first 10 elements");

		for (Tuple2<Integer,String> tuple: pairRDD1.sortByKey(false).take(10)) {
			 System.out.println("<" + tuple._1 + ", " + tuple._2 + ">");
		};
		
		System.out.println("Test cogroup function with 2 JavaPairRDD. Show first 15 elements");

		for (Tuple2<Integer,Tuple2<Iterable<String>, Iterable<String>>> tuple: pairRDD1.sample(true, 0.1).cogroup(pairRDD2.sample(true, 0.1)).take(15)) {
			 System.out.println("<" + tuple._1 + ", " + tuple._2.toString() + ">");
		};
		
		// Hash partition the data
		
		JavaPairRDD<Integer, String> partitionedPairRDD  = pairRDD1.partitionBy(new HashPartitioner(5));
		
		partitionedPairRDD.count();
		
		
	}
	
}
