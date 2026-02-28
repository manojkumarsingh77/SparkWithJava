package com.example;

import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkWithJava")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Spark Session created.");

        spark.stop();
    }
}

package com.example;

import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkWithJava")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Spark Session created: " + spark.version());

        spark.stop();
    }
}