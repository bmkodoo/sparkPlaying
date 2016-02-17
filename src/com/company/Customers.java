package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Nikolai_Karulin on 2/17/2016.
 */
public class Customers {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("RatingsHistogram"));

        Map<Float, Integer> result = sc
                .textFile("file:///SparkCourse/customer-orders.csv")
                .mapToPair(s -> {
                    String[] splited = s.split(",");
                    int customer = Integer.valueOf(splited[0]);
                    float price = Float.valueOf(splited[2]);
                    return new Tuple2<>(customer, price);
                })
                .reduceByKey((x, y) -> x + y)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .collectAsMap();

        Map<Float, Integer> sorted = new TreeMap<>(result);
        for (Float key : sorted.keySet()) {
            System.out.printf("%d: %f\n", result.get(key), key);
        }
    }
}
