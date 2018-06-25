package org.art.spark.task_6;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import static java.lang.Math.*;
import static org.art.spark.task_6.DataUtils.roundToOne;

public class RandomFileGenerator {

    public static final String CSV_HEADER = "student_id,first_name,last_name,age,score\n";
    public static final String COMMA = ",";
    public static final String NEW_LINE = "\n";

    public static final double DOUBLE_PI = 2 * PI;

    //Mean value
    public static final double MEAN = 5;

    //Standard deviation
    public static final double DEVIATION = 1;

    private static Random rnd = new Random(System.currentTimeMillis());

    public static void generateCsvStudentsDataFile(String filePath, long studentsAmount) {
        Tuple2<Double, Double> randomPair;
        File file = new File(filePath);
        int counter = 0;
        try (BufferedWriter out = new BufferedWriter(new FileWriter(file))) {
            out.write(CSV_HEADER);
            do {
                randomPair = getNormalRandomPair(MEAN, DEVIATION);
                counter++;
                out.write(createRow(randomPair._1));
                out.write(createRow(randomPair._2));
            } while (counter < studentsAmount / 2);
            out.flush();
            System.out.println("Number of students: " + counter * 2 + ".");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String createRow(double randomScore) {
        StringBuilder sb = new StringBuilder();
        sb.append("student_id").append(COMMA)
                .append("first_name").append(COMMA)
                .append("last_name").append(COMMA)
                .append("age").append(COMMA)
                .append(randomScore).append(NEW_LINE);
        return sb.toString();
    }

    /**
     * Returns the pair of normally distributed numbers (according
     * to the Box-Muller transformation).
     */
    private static Tuple2<Double, Double> getNormalRandomPair(double mean, double deviation) {
        double z0;
        double z1;
        double u = 0;
        double v = 0;
        while (u == 0 || v == 0) {
            u = rnd.nextDouble();
            v = rnd.nextDouble();
        }
        double cosine = cos(DOUBLE_PI * v);
        double sinus = sqrt(1 - pow(cosine, 2));
        z0 = sqrt(-2 * log(u)) * cosine;
        z1 = sqrt(-2 * log(u)) * sinus;
        return new Tuple2<>(roundToOne(z0 * deviation + mean), roundToOne(z1 * deviation + mean));
    }
}
