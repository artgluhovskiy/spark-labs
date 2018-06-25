package org.art.spark.task_additional;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DatasetCreationDemo {

    static final String JSON_PEOPLE_DIR = "SparkLectures\\src\\main\\resources\\additional\\people.json";

    static final String DB_TABLE_NAME = "people";

    //Database credentials
    static final String DB_URL = "jdbc:mysql://localhost:3306/spark_db?createDatabaseIfNotExist=true";
    static final String USER = "root";
    static final String PASSWORD = "root";

    //SQL statements
    static final String SQL_DELETE_TABLE = "DROP TABLE IF EXISTS people";

    static final String SQL_CREATE_TABLE = "CREATE TABLE people " +
                                           "(id INTEGER NOT NULL PRIMARY KEY, " +
                                           "user_name VARCHAR(15), " +
                                           "city VARCHAR(15));";

    static final String SQL_INSERT_USERS = "INSERT into people (id, user_name, city) VALUES (2, 'Alex', 'Minsk'), (3, 'Tom', 'London');";

    private static void createDbForTest() {

        //Database schema creation
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            stmt = conn.createStatement();
            stmt.execute(SQL_DELETE_TABLE);
            stmt.execute(SQL_CREATE_TABLE);
            stmt.execute(SQL_INSERT_USERS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void runJdbcDatasetExample(SparkSession spark) {
        createDbForTest();

        Properties connectionProps = new Properties();
        connectionProps.put("user", USER);
        connectionProps.put("password", PASSWORD);

        Dataset<Row> jdbcDF = spark.read().jdbc(DB_URL, DB_TABLE_NAME, connectionProps);

        jdbcDF.show();
    }

    private static void runJsonDatasetExample(SparkSession spark) {

        Dataset<Row> peopleData = spark.read().json(JSON_PEOPLE_DIR);

        //The inferred schema visualization
        peopleData.printSchema();
        peopleData.show();

        //Temporary view creation
        peopleData.createOrReplaceTempView("people");

        Dataset<Row> namesDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 20");

        namesDF.show();
    }

    private static void joinDatasetsExample(SparkSession spark) {

        //Get people from json file
        Dataset<Row> jsonPeopleData = spark.read().json(JSON_PEOPLE_DIR);

        //Get people from database
        Properties connectionProps = new Properties();
        connectionProps.put("user", USER);
        connectionProps.put("password", PASSWORD);

        Dataset<Row> dbPeopleData = spark.read()
                .jdbc(DB_URL, DB_TABLE_NAME, connectionProps);

        Dataset<Row> joined = jsonPeopleData.join(dbPeopleData, dbPeopleData.col("id").equalTo(jsonPeopleData.col("id")));

        joined.show();
    }

    private static void runSimpleDatasetExample(SparkSession spark) {

        List<String> data = Arrays.asList("Hello", "from", "list1", "and", "list2",
                "and", "list3", "hello1", "hello2");

        Dataset<String> lines = spark.createDataset(data, Encoders.STRING());

        Dataset<String> helloRDD = lines.filter((FilterFunction<String>) string -> string.contains("hello"));
        Dataset<String> listRDD = lines.filter((FilterFunction<String>) string -> string.contains("list"));
        Dataset<String> helloAndListRDD = helloRDD.union(listRDD);

        helloAndListRDD.show();
    }

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:\\winutils\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("Dataset Creation Demo")
                .master("local[*]")
                .getOrCreate();

        runSimpleDatasetExample(spark);
        runJdbcDatasetExample(spark);
        runJsonDatasetExample(spark);
        joinDatasetsExample(spark);

        spark.stop();
    }
}
