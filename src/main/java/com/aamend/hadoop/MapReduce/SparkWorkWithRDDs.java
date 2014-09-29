package com.aamend.hadoop.MapReduce;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public class SparkWorkWithRDDs {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String oddNumbersArray[] = {"uno","tres","cinco"};
		JavaRDD<String> oddNumbers = sc.parallelize(Arrays.asList(oddNumbersArray));
		
		String evenNumbersArray[] = {"dos","cuatro","seis"};
		JavaRDD<String> evenNumbers = sc.parallelize(Arrays.asList(evenNumbersArray));
		
		JavaRDD<String> numbers = oddNumbers.union(evenNumbers);
		numbers.persist(StorageLevel.MEMORY_ONLY_SER());
		
		System.out.println("There are "  + numbers.count() + " numbers, and these are:");
		
		for (String number: numbers.take((int)numbers.count())) {
			System.out.println(number);
		}
 
		List<String> numbersList = numbers.collect();
		
		for (String number: numbersList){
			System.out.println(number);
		}

		numbers.saveAsTextFile("numbers");
		
	}
}
