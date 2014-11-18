package com.spark.learning.examples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.storage.StorageLevel;

public class SparkActions {


	static public class MeanAcc implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public MeanAcc(Integer acc, Integer count) {

			this.acc = acc;
			this.count = count;

		}
		
		public Integer acc;
		public Integer count;

		public double mean() {
			return acc/(double)count;
		}

	};

	static Function2<MeanAcc, Integer, MeanAcc> addNewElement = new Function2<MeanAcc, Integer, MeanAcc>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public MeanAcc call(MeanAcc meanAcc, Integer number) {
			meanAcc.acc += number;
			meanAcc.count += 1;
			return meanAcc;
		}
	};

	static Function2<MeanAcc, MeanAcc, MeanAcc> combineAccummulators  = new Function2<MeanAcc, MeanAcc, MeanAcc>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public MeanAcc call(MeanAcc meanAcc1, MeanAcc meanAcc2) {
			meanAcc1.acc += meanAcc2.acc; 
			meanAcc1.count += meanAcc2.count;
			return meanAcc1;}
	};

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Spark Actions");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));

		@SuppressWarnings("serial")
		JavaRDD<Integer> numbersSquared = numbers.map(new Function<Integer,Integer>() {
			public Integer call(Integer number) {return number*number;}		
		}
				);

		Integer sum = numbers.reduce(new Function2<Integer,Integer,Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer number1, Integer number2) { return number1 + number2;}
		});
		
		numbers.persist(StorageLevel.MEMORY_ONLY()); // It is used many times  

		displayRDD(numbers,"Numbers");
		displayRDD(numbersSquared,"Numbers Squared");
		System.out.println("The sum of all the numbers RDD elements is:" + sum);

		MeanAcc zeroValue = new MeanAcc(0,0);

		MeanAcc meanAcc = numbers.aggregate(zeroValue, addNewElement, combineAccummulators);

		double average = meanAcc.mean();

		System.out.println("The average of the numbers RDD elements is:" + average);

		// Do the same with the other buffer
		meanAcc = numbersSquared.aggregate(zeroValue, addNewElement, combineAccummulators);
		average = meanAcc.mean();
		
		System.out.println("The average of the numbersSquared RDD elements is:" + average);

		// Another way to compute the average is to create a DoubleRDD and then use their built in function to estimate it
		@SuppressWarnings("serial")
		JavaDoubleRDD doubleNumbers = numbers.mapToDouble(new DoubleFunction<Integer>() {
			public double call(Integer number) {return (double) number*number;} 
		});
		
		System.out.println("The mean of the numbers (squared) RDD using mapToDouble and DoubleFunction is:" + doubleNumbers.mean() + " and their std is : " + doubleNumbers.stdev());
		System.out.println("Their stats are: " + doubleNumbers.stats());
		
		// Test collect to return all the elements in the RDD
		System.out.println("The list of all the RDD elements as a collection is:");
		List<Integer> collection = numbersSquared.collect();
		
		displayList(collection,"Display collection of all elements of the RDD");
		
		System.out.println("The first element of the RDD is: " + numbersSquared.first());
		
		// Take a sample from the RDD
		List<Integer> sampled = numbers.takeSample(false, 3);
		displayList(sampled,"Sampled RDD of 3 elements");

	}

	private static void displayRDD(JavaRDD<Integer> RDD, String message ) {

		System.out.println(message);

		for (Integer number: RDD.take((int)RDD.count())) {
			System.out.print(number);
			System.out.print('-');
		}

		System.out.println();

	}
	
	private static void displayList(List<Integer> list, String message ) {

		System.out.println(message);

		for (Integer number: list) {
			System.out.print(number);
			System.out.print('-');
		}

		System.out.println();

	}

}
