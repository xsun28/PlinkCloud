package org.plinkcloud.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class wordcount {
	public static void main(String args[]){
		SparkConf conf = new SparkConf().setMaster("yarn-client").setAppName("wordcount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("Spark/wordcount");
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String,String>(){
			@Override
			public Iterable<String> call(String line){
				return Arrays.asList(line.split("\\s+"));
			}
		});
		
		JavaPairRDD<String,Integer> counts = words.mapToPair(new PairFunction<String,String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String word){
				return new Tuple2(word, Integer.valueOf(1));
			}
		}
		).reduceByKey(new Function2<Integer,Integer,Integer>(){
			@Override
			public Integer call(Integer a, Integer b){
				return a+b;
			}
		}
		);
		
		counts.saveAsTextFile("Spark/output");
	}
}
