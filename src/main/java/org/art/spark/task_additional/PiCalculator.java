package org.art.spark.task_additional;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculates Pi value in accordance with "Monte Carlo Method".
 */
public class PiCalculator {

    public static double calculatePi(SparkSession spark, int iterations) throws Exception {

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        List<Integer> list = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++) {
            list.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(list);

        int count = dataSet
                .map(integer -> {
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;
                    return (x * x + y * y <= 1) ? 1 : 0;
                })
                .reduce((integer, integer2) -> integer + integer2);

        return 4.0 * count / iterations;
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "c:\\winutils\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("Pi Value Calculation Demo")
                .master("local[*]")
                .getOrCreate();

        int iterations = 1000000;

        double piValue = calculatePi(spark, iterations);

        spark.stop();

        System.out.println("Pi is roughly " + piValue);
    }
}
