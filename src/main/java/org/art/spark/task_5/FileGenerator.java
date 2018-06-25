package org.art.spark.task_5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class FileGenerator {

    public static final String[] REGIONS = new String[]{"Minsk", "Gomel", "Mogilev", "Brest", "Vitebsk", "Grodno"};
    public static final String CSV_HEADER = "start_timest,from,to,duration,region,position\n";
    public static final String START_TIMEST = "2018-11-17 23:59:59,";
    public static final String POSITION = "position\n";
    public static final String TO = "to,";
    public static final String FROM = "from";
    public static final String COMMA = ",";

    private static final Random rnd = new Random(System.currentTimeMillis());

    public static void generateCsvCallPhoneLogFile(String filePath) {

        File file = new File(filePath);
        int counter = 0;
        int iterations;
        try (BufferedWriter out = new BufferedWriter(new FileWriter(file))) {
            out.write(CSV_HEADER);
            //Generating 1 KB file-chunk
            do {
                counter++;
                out.write(createCsvRow());
                out.flush();
            } while (file.length() <= 1024);
            //Generating 100 MB file
            iterations = counter * 102400;
            for (int i = 0; i < iterations; i++) {
                out.write(createCsvRow());
            }
            out.flush();
            System.out.println("File size: " + (file.length() >> 20) + " MB.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int getRndInt(int bound) {
        return rnd.nextInt(bound) + 1;
    }

    private static String createCsvRow() {
        StringBuilder sb = new StringBuilder();
        sb.append(START_TIMEST)
                .append(FROM).append(getRndInt(30)).append(COMMA)
                .append(TO)
                .append(getRndInt(30)).append(COMMA)
                .append(REGIONS[getRndInt(REGIONS.length - 1)]).append(COMMA)
                .append(POSITION);
        return sb.toString();
    }

    public static void converToParquet(SparkSession spark, String srcCsvPath, String dstParquetPath) {
        System.out.println("File converting!");
        Dataset<Row> csvData = spark.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(srcCsvPath);
        csvData.write().format("parquet").save(dstParquetPath);
    }
}
