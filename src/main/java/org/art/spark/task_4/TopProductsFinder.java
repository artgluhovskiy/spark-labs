package org.art.spark.task_4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;
import org.art.spark.task_5.FileGenerator;

import static org.apache.spark.sql.functions.*;

public class TopProductsFinder {

    public static final String JSON_ORDERS_DIR = "SparkLectures\\src\\main\\resources\\task_4\\orders.json";
    public static final String JSON_ITEMS_DIR = "SparkLectures\\src\\main\\resources\\task_4\\items.json";
    public static final String JSON_PRODUCTS_DIR = "SparkLectures\\src\\main\\resources\\task_4\\products.json";

    private static Dataset<Row> findTopProductsPerCategoryWithDataset(SparkSession spark, int topN) {

        Dataset<Row> orders = spark.read().json(JSON_ORDERS_DIR);
        Dataset<Row> items = spark.read().json(JSON_ITEMS_DIR);
        Dataset<Row> products = spark.read().json(JSON_PRODUCTS_DIR);

        WindowSpec win = Window.partitionBy("category_id").orderBy(desc("revenue"));

        Dataset<Row> top = orders.join(items, orders.col("order_id").equalTo(items.col("order_id")))
                .select(items.col("product_id"), items.col("qty"), items.col("cost"))
                .withColumn("sum_product_cost", lit(col("cost").multiply(col("qty"))))
                .drop(items.col("qty")).drop(items.col("cost"))
                .groupBy(items.col("product_id")).agg(sum(col("sum_product_cost")).as("revenue"))
                .join(products, items.col("product_id").equalTo(products.col("product_id")))
                .withColumn("rank", row_number().over(win))
                .filter(col("rank").leq(topN)).drop("product_id");

        top.show();
        top.cache();
        return top;
    }

    private static Dataset<Row> findTopProductsPerCategoryWithSql(SparkSession spark, int topN) {

        Dataset<Row> orders = spark.read().json(JSON_ORDERS_DIR);
        orders.createOrReplaceTempView("orders");

        Dataset<Row> items = spark.read().json(JSON_ITEMS_DIR);
        items.createOrReplaceTempView("items");

        Dataset<Row> products = spark.read().json(JSON_PRODUCTS_DIR);
        products.createOrReplaceTempView("products");

        Dataset<Row> top = spark.sql(
                "SELECT category_id, " +
                            "revenue, " +
                            "product_name, " +
                            "rank " +
                            "FROM (SELECT product_name, " +
                                         "category_id, " +
                                         "revenue, " +
                                         "row_number() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rank " +
                                   "FROM (SELECT product_name, " +
                                                "products.category_id, " +
                                                "SUM(qty * cost) AS revenue " +
                                         "FROM orders INNER JOIN items ON orders.order_id = items.order_id " +
                                                "INNER JOIN products ON items.product_id = products.product_id " +
                                         "GROUP BY products.category_id, product_name) sum_products) prod_rank " +
                        "WHERE rank <= " + topN);
        top.cache();
        top.show();
        return top;
    }

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:\\winutils\\");
        SparkSession spark = SparkSession
                .builder()
                .appName("Find Top N Products per Category Task")
                .master("local[*]")
                .getOrCreate();

        long task4DatasetTime;
        long task4SqlTime;
        int topN = 5;

        //Warming up
        findTopProductsPerCategoryWithDataset(spark,1);
        findTopProductsPerCategoryWithSql(spark, 1);

        //Start testing
        long start;
        start = System.nanoTime();
        Dataset<Row> result4Sql = findTopProductsPerCategoryWithSql(spark, topN);
        task4SqlTime = System.nanoTime() - start;

        start = System.nanoTime();
        Dataset<Row> result4Dataset = findTopProductsPerCategoryWithDataset(spark, topN);
        task4DatasetTime = System.nanoTime() - start;

        result4Sql.show();
        result4Dataset.show();

        spark.stop();

        System.out.println("Task 4. Top " + topN + " products (via Dataset API). Time consumption: " + task4DatasetTime / 1000000 + " ms.");
        System.out.println("Task 4. Top " + topN + " products (via Spark SQL). Time consumption: " + task4SqlTime / 1000000 + " ms.");
    }
}
