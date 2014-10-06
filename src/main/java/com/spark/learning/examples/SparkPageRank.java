package com.spark.learning.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SparkPageRank {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("PageRank");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("data/miniPageRank.txt");

		// create links RDD from input data
		JavaPairRDD<String, List<String>> links = input.mapToPair(new PairFunction<String, String, List<String>>() {
			public Tuple2<String, List<String>> call (String row) { 
				String pageAndLinks[] = row.split(","); // page url separated with the links by a comma 
				return new Tuple2<String, List<String>>(pageAndLinks[0], Arrays.asList(pageAndLinks[1].split(" ")));
			}
		}).partitionBy(new HashPartitioner(10)).persist(StorageLevel.MEMORY_ONLY()); // remember to persist as it is used many times

		// Display links RDD
		System.out.println("Links RDD");
		for (Tuple2<String, List<String>> tuple: links.take((int)links.count())) {
			System.out.println("<" + tuple._1 + " - " + tuple._2.toString() + ">");
		}

		// Create ranks RDD 
		JavaPairRDD<String, Double> ranks = links.mapValues(new Function<List<String>, Double> () {
			public Double call(List<String> strings) {return 1.0;}
		});

		// Iterate in order to obtain the page rank.
		for (int i = 0; i < 10; i++) {
			JavaPairRDD<String, Double> contributions = links.join(ranks).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<List<String>, Double>>, String, Double>() {

				public Iterable<Tuple2<String, Double>> call(Tuple2<String, Tuple2<List<String>, Double>> joined) {

					int length = joined._2._1.size();
					double rank = joined._2._2;

					List<Tuple2<String, Double>> linkRankSums = new ArrayList<Tuple2<String, Double>>(length);

					for (String link: joined._2._1) {
						linkRankSums.add(new Tuple2<String,Double>(link,rank/length));
					}

					return linkRankSums;		
				}

			});

			ranks = contributions.reduceByKey(new Function2<Double, Double, Double>() {
				public Double call(Double a, Double b) { return a + b;}
			}).mapValues(new Function<Double,Double>(){
				public Double call(Double number) { return 0.15 + 0.85*number;}
			});
			
			if (i == 9) {
				
				System.out.println("PageRank Output");
				for (Tuple2<String, Double> tuple: ranks.take((int)ranks.count())) {
					System.out.println("<" + tuple._1 + " - " + tuple._2.toString() + ">");
				}		
			}
		}
	}
}
