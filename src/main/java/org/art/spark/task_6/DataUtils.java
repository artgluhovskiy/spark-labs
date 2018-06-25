package org.art.spark.task_6;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Objects;

import static java.lang.Math.PI;
import static java.lang.Math.sqrt;
import static org.apache.spark.sql.functions.*;

public class DataUtils {

    public static final double FUNC_COEFFICIENT = 1 / sqrt(2 * PI);

    public static final String FIELD_SCORE = "score";
    public static final String FIELD_FREQUENCY = "frequency";
    public static final String FIELD_COUNT = "count";
    public static final String FIELD_PRODUCT = "product";
    public static final String FIELD_NUMERATOR = "numerator";


    public static Dataset<Row> calculateActualFrequencies(Dataset<Row> studentData) {

        return studentData
                .select(col(FIELD_SCORE)).as(Encoders.DOUBLE())
                .withColumn(FIELD_COUNT, lit(1).as(Encoders.INT()))
                .groupBy(col(FIELD_SCORE))
                .agg(sum(col(FIELD_COUNT)).as(FIELD_FREQUENCY))
                .sort(col(FIELD_SCORE))
                .filter(col(FIELD_SCORE).leq(10).and(col(FIELD_SCORE).geq(1)))
                .cache();
    }

    public static Dataset<Double> calculateTheoreticalFrequencies(Dataset<Row> actualScoreData, long size) {

        Objects.requireNonNull(actualScoreData);

        //Mean score value
        double meanScoreValue = calculateMeanScoreValue(actualScoreData);

        //Variance
        double variance = calculateVariance(actualScoreData, meanScoreValue);

        //Standard deviation
        double standardDev = Math.sqrt(variance);

        //Interval
        double interval = 0.1;

        System.out.println("Mean " + meanScoreValue + ", standard dev " + standardDev);

        return calculateFrequencies(actualScoreData, meanScoreValue, standardDev, size, interval);
    }

    /**
     * Returns the mean score value: sum(frequency i * score i) / sum(frequency i).
     */
    private static double calculateMeanScoreValue(Dataset<Row> actualScoreData) {

        Row dataRow = actualScoreData
                .withColumn(FIELD_PRODUCT, col(FIELD_SCORE).multiply(col(FIELD_FREQUENCY)))
                .agg(sum(col(FIELD_PRODUCT)), sum(col(FIELD_FREQUENCY)))
                .first();
        double scoreFreqProductSum = dataRow.getDouble(0);
        long freqSum = dataRow.getLong(1);
        return roundToTwo(scoreFreqProductSum / freqSum);
    }

    /**
     * Returns the variance: sum(score i - mean score value)^2 * frequency i / sum(frequency i).
     */
    private static double calculateVariance(Dataset<Row> actualScoreData, double meanScoreValue) {

        Row dataRow = actualScoreData
                .withColumn(FIELD_NUMERATOR, lit(pow(col(FIELD_SCORE).minus(meanScoreValue), 2)
                        .multiply(col(FIELD_FREQUENCY))))
                .agg(sum(col(FIELD_NUMERATOR)), sum(col(FIELD_FREQUENCY)))
                .first();
        double numeratorSum = dataRow.getDouble(0);
        long freqSum = dataRow.getLong(1);
        return roundToTwo(numeratorSum / freqSum);
    }

    /**
     * Returns the data set of the theoretical frequencies.
     * Additional fields:
     * "t_param":      (score i - mean score value) / standard deviation;
     * "func":         (1 / sqrt(2 * PI)) * e ^ (t_param ^ 2 / (- 2));
     * "theor_freq":   (size * interval / standard deviation) * func(t_param).
     */
    private static Dataset<Double> calculateFrequencies(Dataset<Row> actualScoreData, double meanValue, double standDev,
                                                        long size, double interval) {
        double coef = size * interval / standDev;
        return actualScoreData.drop(FIELD_FREQUENCY)
                .withColumn("t_param", lit((col(FIELD_SCORE).minus(meanValue)).divide(standDev)))
                .withColumn("func", lit(exp(pow(col("t_param"), 2).divide(-2)).multiply(FUNC_COEFFICIENT)))
                .withColumn("theor_freq", lit(col("func").multiply(coef)))
                .select(col("theor_freq")).as(Encoders.DOUBLE());
    }

    public static Vector getFrequencyVector(Dataset<Double> frequencyData) {

        List<Double> freqList = frequencyData.collectAsList();
        double[] freqArray = freqList.stream().mapToDouble(Double::doubleValue).toArray();
        return Vectors.dense(freqArray);
    }

    public static double roundToOne(double value) {
        return Math.round(value * 10) / (double) 10;
    }

    public static double roundToTwo(double value) {
        return Math.round(value * 100) / (double) 100;
    }

    public static Dataset<Row> readStudentsDataFromFile(SparkSession spark, String filePath) {

        Dataset<Row> studentData = spark.read().format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);

        return studentData;
    }
}
