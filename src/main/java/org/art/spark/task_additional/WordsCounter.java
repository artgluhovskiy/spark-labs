package org.art.spark.task_additional;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Counts words in the text file with specified path.
 */
public class WordsCounter {

    public static final String SPLIT_PATTERN = "[\\s]+";

    public static Map<String, Integer> countWords(SparkSession sc, String filePath) {

        return sc.read().textFile(filePath).javaRDD()
                .flatMap(s -> Arrays.asList(s.split(SPLIT_PATTERN)).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .collectAsMap();
    }

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:\\winutils\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("Words Count Calculation Demo")
                .master("local[*]")
                .getOrCreate();

        String wordsFilePath = "SparkLectures\\src\\main\\resources\\additional\\words.txt";

        Map<String, Integer> results = countWords(spark, wordsFilePath);

        System.out.println(results);

        spark.stop();
    }
}
