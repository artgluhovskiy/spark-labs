package org.art.spark.task_5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class TopPhoneCallsFinder {

    private static final String PARQUET_EXT = ".parquet";

    private static Dataset<Row> findTopPhoneCallsPerRegion(SparkSession spark, int topN, String filePath) {

        Dataset<Row> phoneCalls;

        if (filePath.lastIndexOf(PARQUET_EXT) == -1) {
            System.out.println("Reading from csv file.");
            phoneCalls = spark.read().format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(filePath);
        } else {
            System.out.println("Reading from parquet file.");
            phoneCalls = spark.read().format("parquet").load(filePath);
        }

        WindowSpec win = Window.partitionBy("region").orderBy(desc("sum_duration"));

        Dataset<Row> top = phoneCalls.groupBy(col("from"), col("region"))
                .agg(sum(col("duration")).as("sum_duration"))
                .withColumn("rank", row_number().over(win))
                .filter(col("rank").leq(topN));

        top.cache();
        top.show();
        return top;
    }

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        SparkSession spark = SparkSession
                .builder()
                .appName("Find Top N Phone Calls by Duration Task")
                .master("local[*]")
                .getOrCreate();

        long task5CsvTime;
        long task5ParquetTime;
        int topN = 5;

        String csvPhoneCallsDir = "SparkLectures\\src\\main\\resources\\task_5\\csv\\call-logs.csv";
        String parquetPhoneCallDir = "SparkLectures\\src\\main\\resources\\task_5\\parquet\\call-logs.parquet";

        FileGenerator.generateCsvCallPhoneLogFile(csvPhoneCallsDir);
        FileGenerator.converToParquet(spark, csvPhoneCallsDir, parquetPhoneCallDir);

        //Warming up
        findTopPhoneCallsPerRegion(spark, 1, parquetPhoneCallDir);
        findTopPhoneCallsPerRegion(spark, 1, csvPhoneCallsDir);

        //Start testing
        long start;

        start = System.nanoTime();
        findTopPhoneCallsPerRegion(spark, topN, parquetPhoneCallDir);
        task5ParquetTime = System.nanoTime() - start;

        start = System.nanoTime();
        findTopPhoneCallsPerRegion(spark, topN, csvPhoneCallsDir);
        task5CsvTime = System.nanoTime() - start;

        System.out.println("Task 5. Top " + topN + " sum duration calls (reads from csv file). Time consumption: " + task5CsvTime / 1000000000 + " s.");
        System.out.println("Task 5. Top " + topN + " sum duration calls (reads from parquet file). Time consumption: " + task5ParquetTime / 1000000000 + " s.");

        spark.stop();
    }
}
