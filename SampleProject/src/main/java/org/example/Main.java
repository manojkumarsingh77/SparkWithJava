package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SampleParquetDemo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            // -----------------------------------------------------------------
            // 1. Create initial dataset
            // -----------------------------------------------------------------
            List<Person> people = Arrays.asList(
                    new Person(1, "Alice", 30),
                    new Person(2, "Bob", 24),
                    new Person(3, "Catherine", 29),
                    new Person(4, "Daniel", 40),
                    new Person(5, "Emma", 35)
            );

            Dataset<Person> dsPeople = spark.createDataset(people, Encoders.bean(Person.class));

            System.out.println("=== Original Dataset<Person> ===");
            dsPeople.show(false);

            // -----------------------------------------------------------------
            // 2. Filter using Java FilterFunction (NO encoder needed)
            // -----------------------------------------------------------------
            Dataset<Person> adults = dsPeople.filter((FilterFunction<Person>) p -> p.getAge() >= 30);

            System.out.println("=== Adults (age >= 30) ===");
            adults.show(false);

            // -----------------------------------------------------------------
            // 3. Write to Parquet
            // -----------------------------------------------------------------
            String parquetPath = "data/parquet_db/people";

            adults.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(parquetPath);

            System.out.println("Parquet written at: " + parquetPath);

            // -----------------------------------------------------------------
            // 4. Read back
            // -----------------------------------------------------------------
            Dataset<Row> dfRead = spark.read().parquet(parquetPath);

            System.out.println("=== Read Back ===");
            dfRead.show(false);

            // -----------------------------------------------------------------
            // 5. Append new data
            // -----------------------------------------------------------------
            List<Person> newPeople = Arrays.asList(
                    new Person(6, "Frank", 32),
                    new Person(7, "Grace", 28)
            );

            Dataset<Person> dsNew = spark.createDataset(newPeople, Encoders.bean(Person.class));

            // Typed filter
            Dataset<Person> dsAppend = dsNew.filter((FilterFunction<Person>) p -> p.getAge() >= 30);

            dsAppend.write()
                    .mode(SaveMode.Append)
                    .parquet(parquetPath);

            System.out.println("Appended new rows.");

            // -----------------------------------------------------------------
            // 6. Create database + table WITHOUT IntelliJ warnings
            // -----------------------------------------------------------------
            String databaseName = "demo_db";
            spark.sql("CREATE DATABASE IF NOT EXISTS " + databaseName);

            String absPath = new java.io.File(parquetPath)
                    .getAbsolutePath()
                    .replace("\\", "/");

            String parquetUri = "file://" + absPath;

            String createTable =
                    "CREATE TABLE IF NOT EXISTS " + databaseName + ".people_parquet " +
                            "USING PARQUET LOCATION '" + parquetUri + "'";

            spark.sql(createTable);

            System.out.println("Table created for parquet: " + parquetUri);

            // -----------------------------------------------------------------
            // 7. Query table
            // -----------------------------------------------------------------
            Dataset<Row> sqlResult =
                    spark.sql("SELECT * FROM " + databaseName + ".people_parquet ORDER BY id");

            System.out.println("=== Query Result ===");
            sqlResult.show(false);

        } finally {
            spark.stop();
        }
    }
}
