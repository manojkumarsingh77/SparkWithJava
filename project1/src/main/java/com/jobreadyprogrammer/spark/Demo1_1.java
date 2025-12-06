package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Demo application to explain:
 *  - Bucketing on customer_id
 *  - Adaptive Query Execution (AQE) with skew handling
 *
 * Prerequisites:
 *  - Run generate_data.py to create data/customers.csv and data/transactions.csv
 *  - Spark 3.x
 */
public class Demo1_1 {

    public static void main(String[] args) {

        // 1. Create SparkSession
        SparkSession spark = new SparkSession.Builder()
                .appName("Bucketing and AQE Demo")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "16")        // for demo
                .config("spark.sql.adaptive.enabled", "false")       // AQE disabled for now
                .config("spark.sql.adaptive.skewJoin.enabled", "false")
                .config("spark.sql.warehouse.dir", "spark-warehouse") // local warehouse
                // Optional: disable broadcast join to better see bucketed / shuffle behavior
                // .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 2. Load the source CSV data
        Dataset<Row> customersDf = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/customers.csv");
        Dataset<Row> transactionsDf = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/transactions.csv");

        System.out.println("=== Sample customers ===");
        customersDf.show(5, false);

        System.out.println("=== Sample transactions ===");
        transactionsDf.show(5, false);

        // 3. Normal join without using bucket tables (baseline)
        System.out.println("=== Normal join (no explicit bucketing) ===");
        Dataset<Row> normalJoin = runNormalJoin(customersDf, transactionsDf);
        normalJoin.explain("formatted");
        normalJoin.show(10, false);

        // Time the normal join materialization (full job)
        long normalStart = System.nanoTime();
        long normalCount = normalJoin.count();   // action to materialize the full join
        long normalEnd = System.nanoTime();
        double normalMs = (normalEnd - normalStart) / 1_000_000.0;

        System.out.println("=== Normal join metrics ===");
        System.out.println("Row count (normal join): " + normalCount);
        System.out.printf("Execution time (normal join): %.2f ms (%.2f s)%n",
                normalMs, normalMs / 1000.0);

        // 4. Write bucketed tables on customer_id
        writeBucketedTables(spark, customersDf, transactionsDf);

        // 5. Read bucketed tables and join again
        Dataset<Row> customersBucketed = spark.table("demo_customers_bucketed");
        Dataset<Row> transactionsBucketed = spark.table("demo_transactions_bucketed");

        System.out.println("=== Join using bucketed tables ===");
        Dataset<Row> bucketedJoin = runBucketedJoin(customersBucketed, transactionsBucketed);
        bucketedJoin.explain("formatted");
        bucketedJoin.show(10, false);

        // Time the bucketed join materialization (full job)
        long bucketedStart = System.nanoTime();
        long bucketedCount = bucketedJoin.count();   // action to materialize the full join
        long bucketedEnd = System.nanoTime();
        double bucketedMs = (bucketedEnd - bucketedStart) / 1_000_000.0;

        System.out.println("=== Bucketed join metrics ===");
        System.out.println("Row count (bucketed join): " + bucketedCount);
        System.out.printf("Execution time (bucketed join): %.2f ms (%.2f s)%n",
                bucketedMs, bucketedMs / 1000.0);

        // Simple comparison summary
        System.out.println("=== Comparison Summary ===");
        System.out.printf("Normal join   : %.2f ms (%.2f s), rows = %d%n",
                normalMs, normalMs / 1000.0, normalCount);
        System.out.printf("Bucketed join : %.2f ms (%.2f s), rows = %d%n",
                bucketedMs, bucketedMs / 1000.0, bucketedCount);

        spark.stop();
    }

    private static Dataset<Row> loadCustomers(SparkSession spark) {
        return spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/customers.csv");
    }

    private static Dataset<Row> loadTransactions(SparkSession spark) {
        return spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/transactions.csv");
    }

    /**
     * Simple join without any explicit bucketing.
     * This is only to contrast with the bucketed join.
     */
    private static Dataset<Row> runNormalJoin(Dataset<Row> customersDf,
                                              Dataset<Row> transactionsDf) {

        Dataset<Row> joined = customersDf.alias("c")
                .join(
                        transactionsDf.alias("t"),
                        customersDf.col("customer_id").equalTo(transactionsDf.col("customer_id"))
                )
                .select(
                        customersDf.col("customer_id"),
                        customersDf.col("customer_name"),
                        customersDf.col("segment"),
                        customersDf.col("city"),
                        transactionsDf.col("transaction_id"),
                        transactionsDf.col("txn_date"),
                        transactionsDf.col("txn_amount"),
                        transactionsDf.col("channel"),
                        transactionsDf.col("product")
                );

        return joined;
    }

    /**
     * Writes bucketed tables for customers and transactions on customer_id.
     * This uses Hive-style bucketed tables in the Spark warehouse.
     */
    private static void writeBucketedTables(SparkSession spark,
                                            Dataset<Row> customersDf,
                                            Dataset<Row> transactionsDf) {

        // Use a simple "demo" database
        spark.sql("CREATE DATABASE IF NOT EXISTS demo_db1");
        spark.catalog().setCurrentDatabase("demo_db1");

        // Drop old tables if they exist
        spark.sql("DROP TABLE IF EXISTS demo_customers_bucketed");
        spark.sql("DROP TABLE IF EXISTS demo_transactions_bucketed");

        int numBuckets = 8; // choose a small number for demo

        // Write bucketed customers table
        customersDf.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(numBuckets, "customer_id")
                .sortBy("customer_id")
                .saveAsTable("demo_customers_bucketed");

        // Write bucketed transactions table
        transactionsDf.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(numBuckets, "customer_id")
                .sortBy("customer_id")
                .saveAsTable("demo_transactions_bucketed");

        System.out.println("=== Bucketed tables written in demo_db1 ===");
        System.out.println("Tables:");
        spark.sql("SHOW TABLES IN demo_db1").show(false);
    }

    /**
     * Join using the bucketed tables.
     * Spark can take advantage of compatible bucketing (same numBuckets and key)
     * when broadcast is disabled and query planner decides to use bucketed joins.
     */
    private static Dataset<Row> runBucketedJoin(Dataset<Row> customersBucketed,
                                                Dataset<Row> transactionsBucketed) {

        Dataset<Row> joined = customersBucketed.alias("c")
                .join(
                        transactionsBucketed.alias("t"),
                        customersBucketed.col("customer_id")
                                .equalTo(transactionsBucketed.col("customer_id"))
                )
                .select(
                        customersBucketed.col("customer_id"),
                        customersBucketed.col("customer_name"),
                        customersBucketed.col("segment"),
                        customersBucketed.col("city"),
                        transactionsBucketed.col("transaction_id"),
                        transactionsBucketed.col("txn_date"),
                        transactionsBucketed.col("txn_amount"),
                        transactionsBucketed.col("channel"),
                        transactionsBucketed.col("product")
                );

        return joined;
    }
}
