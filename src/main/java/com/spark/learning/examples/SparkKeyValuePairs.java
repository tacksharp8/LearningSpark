package com.spark.learning.examples;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkKeyValuePairs {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("data/textFile.txt");

		@SuppressWarnings("serial")
		PairFunction<String, String, String> keyValFunction = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String string) { 
				return new Tuple2<String, String>(Integer.toString(string.length()),string);
			}

		};

		JavaPairRDD<String, String> pairRDD = input.mapToPair(keyValFunction);
		
	

		System.out.println("The resulting key value pair RDD is (only 5 elements): ");

		//List<Tuple2<String,String>> list4Pairs = pairRDD.take(5);

		for (Tuple2<String,String> tuple: pairRDD.take((int) pairRDD.count())) {
			System.out.println("(" + tuple._1 + ", " + tuple._2 + ")");
		}

		// Parallelizing pairs from an in-memory collection
		ArrayList<Tuple2<String,String>> tupleList = new ArrayList<Tuple2<String,String>>(); 
		tupleList.add(new Tuple2<String,String>("hola","mundo"));
		tupleList.add(new Tuple2<String,String>("hello","world"));
		tupleList.add(new Tuple2<String,String>("ciao","mondo"));

		JavaPairRDD<String, String> tuples = sc.parallelizePairs(tupleList);


		System.out.println("Parallelizing pairs from an in-memory collection");

		for (Tuple2<String,String> tuple: tuples.take((int) tuples.count())) {
			System.out.println("(" + tuple._1 + ", " + tuple._2 + ")");
		}
		
		@SuppressWarnings("serial")
		Function<Tuple2<String, String>, Boolean> filterValue = new Function<Tuple2<String, String>, Boolean>() {
			public Boolean call(Tuple2<String, String> tuple) {return tuple._2.split(" ").length > 3;}
		};
		
		System.out.println("The number of filtered elements of the pairRDD is: " + pairRDD.filter(filterValue).count() + " out of: " + pairRDD.count());
		
		
		// Use the flatMap and reduceByKey to count the number of words for each type of word.
	
		@SuppressWarnings("serial")
		JavaRDD<String> flatInput = input.flatMap(new FlatMapFunction<String,String>() {
			public Iterable<String> call(String string) { return Arrays.asList(string.split(" ")); }
			}
		);
		
		@SuppressWarnings("serial")
		JavaPairRDD<String, Integer> wordCountPair = flatInput.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String,Integer> call(String string) { return new Tuple2<String, Integer>(string,1);}
		});
		
		@SuppressWarnings("serial")
		JavaPairRDD<String,Integer> wordCountPairReduced = wordCountPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {return a + b; }
		});
		
		for (Tuple2<String, Integer> tuple: wordCountPairReduced.take(10)) {
			System.out.println("(" + tuple._1 + ", " + tuple._2 + ")");
		}

	}

}
