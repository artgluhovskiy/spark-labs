package org.art.spark.task_6;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.art.spark.task_6.DataUtils.*;
import static org.art.spark.task_6.RandomFileGenerator.generateCsvStudentsDataFile;

public class HypothesisTester {

    public static void testHypothesis(Vector actualVector, Vector theoreticalVector) {

        ChiSqTestResult statResults;
        if (actualVector.size() != theoreticalVector.size()) {
            System.out.println("Calculation failed! Wrong initial data! Vector sizes are not equal.");
        } else {
            statResults = Statistics.chiSqTest(actualVector, theoreticalVector);
            System.out.println("Hypothesis test results:\n");
            System.out.println("*** degrees of freedom: " + statResults.degreesOfFreedom() + "\n");
            System.out.println("*** p-Value: " + statResults.pValue() + "\n");
            System.out.println("*** statistic: " + statResults.statistic() + "\n");
            System.out.println("*** null hypothesis: " + statResults.nullHypothesis() + "\n");
        }
    }

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:\\winutils\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("Hypothesis Testing Task")
                .master("local[*]")
                .getOrCreate();

        long studentsAmount = 1000;

        String filePath = "SparkLectures\\src\\main\\resources\\task_6\\random-student-data-file.csv";

        generateCsvStudentsDataFile(filePath, studentsAmount);
        Dataset<Row> studentsData = readStudentsDataFromFile(spark, filePath);

        Dataset<Row> actualScoreFrequencyData = calculateActualFrequencies(studentsData);
        Dataset<Double> theoreticalFrequencies = calculateTheoreticalFrequencies(actualScoreFrequencyData, studentsAmount);

        Dataset<Double> actualFrequencies = actualScoreFrequencyData.select(col(FIELD_FREQUENCY)).as(Encoders.DOUBLE());

        Vector actualFreqVector = getFrequencyVector(actualFrequencies);
        Vector theoreticalFreqVector = getFrequencyVector(theoreticalFrequencies);

        HypothesisTester.testHypothesis(actualFreqVector, theoreticalFreqVector);
    }
}
