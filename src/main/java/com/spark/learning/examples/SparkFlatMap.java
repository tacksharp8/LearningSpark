package com.spark.learning.examples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;


public class SparkFlatMap {
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkFlatMap");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String[] sentences = {"uno dos tres", "one two three", "uno due tre", "eins zwei drei"};
		JavaRDD<String> numbers = sc.parallelize(Arrays.asList(sentences));
				
		System.out.println("There are "  + numbers.count() + " sentences, and these are:");
		
		for (String number: numbers.take((int)numbers.count())) {
			System.out.print(number);
			System.out.print('-');
		}
		
		System.out.println();  

		// Top-level function 
		
		// In-line function
		@SuppressWarnings("serial")
		JavaRDD<String> individualNumbers = numbers.flatMap(new FlatMapFunction<String,String>() {
			public Iterable<String> call(String sentence) { return Arrays.asList(sentence.split(" ")); }
		}
				);
		
		System.out.println("Intividual words are: ");
		
		for (String word: individualNumbers.take((int)individualNumbers.count())) {
			System.out.print(word);
			System.out.print('-');
		}
	}
}
