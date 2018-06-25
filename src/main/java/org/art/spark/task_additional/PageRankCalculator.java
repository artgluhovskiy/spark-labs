package org.art.spark.task_additional;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Google Page Rank algorithm implementation.
 */
public class PageRankCalculator {

    private static final Pattern SPLIT_PATTERN = Pattern.compile("\\s+");

    //Number of iterations
    private static final int ITERATIONS = 10;

    public static List<Tuple2<String, Double>> calculatePageRanks(SparkSession spark, String filePath) {

        JavaRDD<String> lines = spark.read().textFile(filePath).javaRDD();
        JavaPairRDD<String, Iterable<String>> links = lines
                .mapToPair(s -> {
                    String[] parts = SPLIT_PATTERN.split(s);
                    return new Tuple2<>(parts[0], parts[1]);
                })
                .distinct()
                .groupByKey()
                .cache();

        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        for (int current = 0; current < ITERATIONS; current++) {
            JavaPairRDD<String, Double> contribs = links
                    .join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2 / urlCount));
                        }
                        return results.iterator();
                    });
            ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(sum -> round(0.15 + sum * 0.85));
        }
        return ranks.collect();
    }

    private static double round(double val) {
        return Math.round(val * 100) / (double) 100;
    }

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:\\winutils\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("Page Rank Calculation Demo")
                .master("local[*]")
                .getOrCreate();

        String urlsFilePath = "SparkLectures\\src\\main\\resources\\additional\\urls.txt";

        List<Tuple2<String, Double>> ranks = PageRankCalculator.calculatePageRanks(spark, urlsFilePath);

        spark.stop();

        ranks.forEach(t -> System.out.println(t._1() + " has rank: " + t._2() + "."));
    }
}
