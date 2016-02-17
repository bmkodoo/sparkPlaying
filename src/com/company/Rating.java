package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

public class Rating {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("RatingsHistogram"));

        JavaRDD<String> textFile = sc.textFile("file:///SparkCourse/ml-100k/u.data");
        JavaRDD<String> words = textFile.map(s -> s.split("\\s+")[2]);
        Map<String, Long> result = words.countByValue();

        for (String key : result.keySet()) {
            System.out.printf("%s: %d\n", key, result.get(key));
        }
    }
}
