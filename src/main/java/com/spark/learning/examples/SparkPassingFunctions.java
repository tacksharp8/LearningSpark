package com.spark.learning.examples;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;





public class SparkPassingFunctions {

	@SuppressWarnings("serial")
	static class ContainsWord implements Function<String, Boolean> {
	public Boolean call(String word) { return word.contains("zero"); }
	}
			
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String numbersArray[] = {"zero", "uno", "tres", "cinco", "zero", "siete", "zero", "zero"};
		JavaRDD<String> numbers = sc.parallelize(Arrays.asList(numbersArray));
		
		System.out.println("There are "  + numbers.count() + " numbers, and these are:");
		
		for (String number: numbers.take((int)numbers.count())) {
			System.out.println(number);
		}
		
		JavaRDD<String> filtNumbers = numbers.filter(new ContainsWord());
				
		System.out.println("There are "  + filtNumbers.count() + " filtered numbers, and these are:");
		
		for (String number: filtNumbers.take((int)numbers.count())) {
			System.out.println(number);
		}

	}

}
