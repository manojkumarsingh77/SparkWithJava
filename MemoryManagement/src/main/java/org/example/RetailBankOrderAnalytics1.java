package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.functions;

import java.sql.*;
import java.util.Properties;

public class RetailBankOrderAnalytics1 {
    public static void main(String[] args) {
        long startTime = System.nanoTime();

        // NOTE:
        // 1) We use JavaSerializer here to avoid Kryo reflective access to JDK internals
        //    (java.nio.ByteBuffer / sun.util.calendar.ZoneInfo) that cause InaccessibleObjectException on Java 17
        //    when --add-opens is not provided to the driver JVM.
        // 2) We avoid to_date/toJavaDate and instead produce trade_date as a STRING using date_format(...).
        //    This prevents Spark from invoking internal JDK classes (ZoneInfo) when creating java.sql.Date
        //    during row materialization / JDBC writes.
        //
        // If you want to use Kryo for performance in production, revert spark.serializer to Kryo and
        // start the JVM that runs the driver with the following VM flags:
        //   --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
        //   --add-opens=java.base/java.nio=ALL-UNNAMED
        //   --add-opens=java.base/java.lang=ALL-UNNAMED
        //   --add-opens=java.base/java.util=ALL-UNNAMED
        //   --add-opens=java.base/java.io=ALL-UNNAMED
        //   --add-opens=java.base/java.math=ALL-UNNAMED
        //

        SparkConf conf = new SparkConf()
                .setAppName("RetailBankOrderAnalytics")
                .setMaster("local[*]")
                // Use Java serializer to avoid Kryo reflection problems in local/IDE runs
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                // tuning for local runs
                .set("spark.sql.shuffle.partitions", "12");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            // read JSON (multiline)
            String jsonPath = "src/main/resources/orders.json";
            Dataset<Row> orders = spark.read().option("multiline", true).json(jsonPath);
            orders.printSchema();
            orders.show(5, false);

            // basic cleaning: ensure price and quantity exist
            Dataset<Row> cleaned = orders.filter((FilterFunction<Row>) row ->
                    row.getAs("price") != null && row.getAs("quantity") != null);

            // Enrich:
            //  - trade_value = price * quantity
            //  - trade_date_str = date formatted as yyyy-MM-dd (STRING) to avoid java.sql.Date conversions
            Dataset<Row> enriched = cleaned
                    .withColumn("trade_value", functions.col("price").multiply(functions.col("quantity")))
                    // keep timestamp column as-is; create trade_date_str as formatted STRING
                    .withColumn("trade_date_str", functions.date_format(functions.col("timestamp"), "yyyy-MM-dd"));

            // Aggregate on security_id and trade_date_str (STRING)
            Dataset<Row> aggregated = enriched.groupBy("security_id", "trade_date_str")
                    .agg(functions.sum("quantity").alias("total_volume"),
                            functions.avg("price").alias("avg_price"))
                    .orderBy("trade_date_str");

            aggregated.show(20, false);

            // JDBC DB create/write (unchanged)
            String dbName = "course_data";
            String adminUrl = "jdbc:postgresql://localhost:5432/postgres";
            String adminUser = "manojkumar";
            String adminPass = "password";

            try (Connection conn = DriverManager.getConnection(adminUrl, adminUser, adminPass)) {
                String check = "SELECT 1 FROM pg_database WHERE datname = ?";
                try (PreparedStatement ps = conn.prepareStatement(check)) {
                    ps.setString(1, dbName);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            try (Statement st = conn.createStatement()) {
                                st.executeUpdate("CREATE DATABASE " + dbName);
                                System.out.println("Created DB " + dbName);
                            }
                        } else {
                            System.out.println("DB exists: " + dbName);
                        }
                    }
                }
            } catch (SQLException e) {
                System.err.println("DB create failed (continuing): " + e.getMessage());
                // continue execution even if DB create fails locally
            }

            String dbUrl = "jdbc:postgresql://localhost:5432/" + dbName;
            Properties props = new Properties();
            props.put("user", "manojkumar");
            props.put("password", "password");
            props.put("driver", "org.postgresql.Driver");

            // Write aggregated result to JDBC. Note: trade_date_str is a STRING -> avoids java.sql.Date conversion.
            aggregated.write().mode(SaveMode.Overwrite).jdbc(dbUrl, "project1", props);

            System.out.println("Spark job completed successfully.");
        } catch (Exception e) {
            System.err.println("Spark job failed: " + e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            try { spark.stop(); } catch (Exception ignore) {}
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("-------------------------------------------------");
        System.out.println("Spark Job Execution Duration: " + durationMs + " ms");
        System.out.println("Execution Duration (seconds): " + (durationMs / 1000.0));
        System.out.println("-------------------------------------------------");
    }
}
