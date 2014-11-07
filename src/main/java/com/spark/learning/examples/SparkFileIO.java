package com.spark.learning.examples;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkFileIO {

	public static void main(String[] args) {

	    // Create a Java Spark Context. 
	    SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// get arguments from the input	
		String input = args[0];
		String output = args[1];
		
		File inputFile = new File(input);
		
		if (inputFile.exists()) {
		
			if (inputFile.isFile()) {
				
				JavaRDD<String> singleFile = sc.textFile(input);
				displayRDD(singleFile, "Single text file:");
			
				singleFile.saveAsTextFile(output);
			}
			else {
				
				JavaPairRDD<String,String> multipleFiles = sc.wholeTextFiles(input);
				for (int i = 0; i < multipleFiles.count(); i++) {
					displayPairRDD(multipleFiles, "Mutiple text files");
				}

				multipleFiles.saveAsTextFile(output);
			}
		}
		else
			System.out.println("The provided argument does not match a input file or directory");
	}
	
	private static void displayRDD(JavaRDD<String> RDD, String message ) {

		System.out.println(message);
		
		for (String string: RDD.take((int)RDD.count())) {
			System.out.println(string);
		}
		
		System.out.println();

	}
	
	private static void displayPairRDD(JavaPairRDD<String,String> RDD, String message ) {

		System.out.println(message);
		
		for (Tuple2<String,String> tuple: RDD.take((int)RDD.count())) {
			System.out.println("Filename: " + tuple._1);
			System.out.println("File Content: \n" + tuple._2);
		}
		
		System.out.println();
	}
}
