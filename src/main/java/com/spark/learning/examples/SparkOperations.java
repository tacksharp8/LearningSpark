package com.spark.learning.examples;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class SparkOperations { 
	
	@SuppressWarnings("serial")
	private static class square implements Function<Integer,Integer> {
		public Integer call(Integer number) { return number*number; }
		} 
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Spark Operations");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));
				
	
		// Top-level function 
		
		JavaRDD<Integer> numbersSquared = numbers.map(new square());
				
		JavaRDD<Integer> unionRDD = numbers.union(numbersSquared);
		
		displayRDD(numbers,"Numbers");
		displayRDD(numbersSquared,"Numbers Squared");
		displayRDD(unionRDD,"Union of two RDDs");
		displayRDD(unionRDD.distinct(),"Distinct Union RDDs");
		displayRDD(unionRDD.subtract(numbers),"Remove Numbers from Union RDDs");
		displayRDD(numbers.intersection(numbersSquared),"Intersect numbers and theirs squares");
		
		System.out.println("print first 20 cartesian pairs");
		
		for (Tuple2<Integer, Integer> pair: numbers.cartesian(numbers).take(20)) {
			System.out.print(pair.toString());
			System.out.print('-');
		}
	}

	private static void displayRDD(JavaRDD<Integer> RDD, String message ) {

		System.out.println(message);
		
		for (Integer number: RDD.take((int)RDD.count())) {
			System.out.print(number);
			System.out.print('-');
		}
		
		System.out.println();

	}
	
	
}
