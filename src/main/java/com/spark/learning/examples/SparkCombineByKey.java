package com.spark.learning.examples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkCombineByKey {
	
	@SuppressWarnings("serial")
	static public class MeanAcc implements Serializable {

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

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("data/textFile.txt");
		
		@SuppressWarnings("serial")
		JavaRDD<String> flatInput = input.flatMap(new FlatMapFunction<String,String>() {
			public Iterable<String> call(String string) { return Arrays.asList(string.split(" ")); }
			}
		);
		
		@SuppressWarnings("serial")
		JavaPairRDD<String, Integer> wordCountPair = flatInput.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String,Integer> call(String string) { return new Tuple2<String, Integer>(string,(int)(Math.random()*10));} // Notice the random number just for testing purposes
		});
		
		
		@SuppressWarnings("serial")
		Function<Integer, MeanAcc> createCombiner = new Function<Integer, MeanAcc>() {
			public MeanAcc call(Integer number) { return new MeanAcc(number,1);}
		};
		
		
		@SuppressWarnings("serial")
		Function2<MeanAcc, Integer, MeanAcc> mergeValue = new Function2<MeanAcc, Integer, MeanAcc>() {
			public MeanAcc call(MeanAcc mean, Integer number) { 
				mean.acc += number;
				mean.count += 1;
				return mean;
			}
		};
		
		@SuppressWarnings("serial")
		Function2<MeanAcc, MeanAcc, MeanAcc> mergeCombiners = new Function2<MeanAcc, MeanAcc, MeanAcc>() {
			public MeanAcc call (MeanAcc mean1, MeanAcc mean2) { 
				mean1.acc += mean2.acc;
				mean1.count += mean2.count;
				return mean1;
			}
		};
		
		JavaPairRDD<String,MeanAcc> meanAccByKey = wordCountPair.combineByKey(createCombiner, mergeValue, mergeCombiners); 

		// My approach to display the data using mapValues
		System.out.println("My approach to display the data using mapValues");
		@SuppressWarnings("serial")
		JavaPairRDD<String,Double> meanByKey = meanAccByKey.mapValues(new Function<MeanAcc, Double>() {
			public Double call(MeanAcc mean) { return mean.mean();}
		}); 
		

		for (Tuple2<String, Double> tuple: meanByKey.take(10)) {
			System.out.println("(" + tuple._1 + ", " + tuple._2 + ")");
		}
		
		// The book approach by converting the JavaPairRDD into a map
		System.out.println("The book approach by converting the JavaPairRDD into a map");
		Map<String, MeanAcc> meanMap = meanAccByKey.collectAsMap();

		int counter = 0;
		for (Entry<String, MeanAcc> entry: meanMap.entrySet()) {
			System.out.println("(" + entry.getKey() + ", " + entry.getValue().mean() + ")");
			counter++;
			if (counter >= 10) {
				break;
			};
		}
	
		
	}

}
