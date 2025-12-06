package com.jobreadyprogrammer.spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application {
	
	public static void main(String args[]) throws InterruptedException {
		
		// Create a session
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DB")
				.master("local")
				.getOrCreate();

		// get data
		Dataset<Row> df = spark.read().format("csv")
			.option("header", true)
			.load("src/main/resources/name_and_comments.txt");
		
		df.show();
		
		// Transformation
		df = df.withColumn("full_name", 
				concat(df.col("last_name"), lit(", "), df.col("first_name")))
				.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());
		
		// Write to destination
		String dbConnectionUrl = "jdbc:postgresql://localhost:5432/course_data";
		Properties prop = new Properties();
		prop.setProperty("driver", "org.postgresql.Driver");
		prop.setProperty("user", "manojkumar");   // <-- your actual DB role
		prop.setProperty("password", "password"); // <-- same as ALTER ROLE above

	    
	    df.write()
	    	.mode(SaveMode.Overwrite)
	    	.jdbc(dbConnectionUrl, "project1", prop);
	}
}