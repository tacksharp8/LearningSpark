package com.spark.learning.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkEmptyMain {

	public static void main(String[] args) {

	    // Create a Java Spark Context. 
	    SparkConf conf = new SparkConf().setAppName("Spark Empty Main");
		@SuppressWarnings("unused")
		JavaSparkContext sc = new JavaSparkContext(conf);

	    System.out.println("Spark Empty Main");
	    System.out.println("This code does nothing. Exiting...");
	   
	}
	
}
