package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class RetailBankOrderAnalytics3 {

    public static void main(String[] args) {
        long globalStart = System.nanoTime();

        // ---------- USER-CONFIGURABLE EXPERIMENT OPTIONS ----------
        boolean useKryo = false;        // set true to test Kryo (requires --add-opens JVM flags in IntelliJ/run)
        int syntheticRows = 2_000_000;  // number of synthetic rows to generate (increase to stress memory)
        int repartitionBuckets = 50;    // force repartition to cause shuffle pressure
        boolean persistMemoryAndDisk = true; // persist to MEMORY_AND_DISK to observe spills
        boolean writeToParquet = true; // set true to write aggregated result to parquet warehouse folder
        String warehousePath = "./warehouse/aggregated_parquet"; // output path for parquet files
        // ---------------------------------------------------------

        // Build SparkConf with toggles for serializers and memory-related settings
        SparkConf conf = new SparkConf()
                .setAppName("RetailBankOrderAnalyticsMemoryDemo")
                .setMaster("local[*]")
                .set("spark.sql.shuffle.partitions", "12") // small for local; change for experiments
                // tuning defaults (override below if using Kryo)
                .set("spark.kryoserializer.buffer", "64m")
                .set("spark.kryoserializer.buffer.max", "512m");

        if (useKryo) {
            // If you set useKryo = true, please add JVM flags when launching:
            // --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.kryo.registrationRequired", "false")
                    .set("spark.kryoserializer.buffer", "64m")
                    .set("spark.kryoserializer.buffer.max", "512m");
        } else {
            // Java serializer avoids reflective access to JDK internals on modern JDKs (safe default)
            conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        }

        // Memory tuning knobs (change these across runs to demonstrate effects)
        conf.set("spark.driver.memory", "3g");
        conf.set("spark.executor.memory", "2g");
        conf.set("spark.memory.fraction", "0.6");
        conf.set("spark.memory.storageFraction", "0.4");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("=== RUN CONFIGURATION ===");
        System.out.println("useKryo = " + useKryo);
        System.out.println("syntheticRows = " + syntheticRows);
        System.out.println("repartitionBuckets = " + repartitionBuckets);
        System.out.println("persistMemoryAndDisk = " + persistMemoryAndDisk);
        System.out.println("writeToParquet = " + writeToParquet);
        System.out.println("warehousePath = " + warehousePath);
        System.out.println("SparkConf: " + spark.sparkContext().getConf().getAll());

        try {
            // ---------------- Synthetic dataset generator (distributed) ----------------
            // Use spark.range to create a large distributed DF without heavy I/O.
            Dataset<Row> synthetic = spark.range((long) syntheticRows)
                    .withColumn("security_id", functions.expr("cast((id % 1000) as int)"))
                    .withColumn("price", functions.expr("cast(rand()*100.0 as double)"))
                    .withColumn("quantity", functions.expr("cast(1 + floor(rand()*100) as int)"))
                    // create timestamp-ish column as string-compatible expression
                    .withColumn("timestamp", functions.expr("timestampadd(second, cast(rand()*31536000 as int), current_timestamp())"))
                    .drop("id");

            System.out.println("Synthetic dataset created. Preview:");
            synthetic.printSchema();
            synthetic.show(5, false);

            // ---------------- Pre-processing: filter / optional caching ----------------
            Dataset<Row> cleaned = synthetic.filter((FilterFunction<Row>) row ->
                    row.getAs("price") != null && row.getAs("quantity") != null);

            // force repartition to create shuffle pressure (experiment knob)
            Dataset<Row> repartitioned = cleaned.repartition(repartitionBuckets, functions.col("security_id"));

            if (persistMemoryAndDisk) {
                repartitioned.persist(StorageLevel.MEMORY_AND_DISK());
            } else {
                repartitioned.persist(StorageLevel.MEMORY_ONLY());
            }

            long t0 = System.nanoTime();
            // materialize cache so we observe memory effects (first action)
            long count = repartitioned.count();
            long t1 = System.nanoTime();
            System.out.printf("Materialized repartitioned rows: %d (time ms = %d)%n", count, (t1 - t0) / 1_000_000);

            // Print JVM runtime memory (useful for demonstrating GC / heap usage)
            printJvmMemory("After materialize");

            // ---------------- Enrichment & aggregation ----------------
            // Create trade_value and trade_date_str (STRING) to avoid java.sql.Date creation internals
            Dataset<Row> enriched = repartitioned
                    .withColumn("trade_value", functions.col("price").multiply(functions.col("quantity")))
                    .withColumn("trade_date_str", functions.date_format(functions.col("timestamp"), "yyyy-MM-dd"));

            // Optional small broadcast example (small lookup table) - broadcast a Java List<Row>
            List<Row> currencyList = Arrays.asList(
                    RowFactory.create("USD", "US Dollar"),
                    RowFactory.create("INR", "Indian Rupee"),
                    RowFactory.create("EUR", "Euro")
            );

            StructType currencySchema = new StructType(new StructField[]{
                    new StructField("code", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("name", DataTypes.StringType, false, Metadata.empty())
            });

            Dataset<Row> currenciesDf = spark.createDataFrame(currencyList, currencySchema);
            // If you need a broadcast for the small table, broadcast the Java collection (serializable)
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            Broadcast<List<Row>> bcCurrencies = jsc.broadcast(currencyList);

            // Aggregation on security_id and trade_date_str (STRING) to avoid Java Date internals
            Dataset<Row> aggregated = enriched.groupBy("security_id", "trade_date_str")
                    .agg(functions.sum("quantity").alias("total_volume"),
                            functions.avg("price").alias("avg_price"))
                    .orderBy("trade_date_str");

            long tAggStart = System.nanoTime();
            aggregated.show(20, false); // action that triggers actual computation
            long tAggEnd = System.nanoTime();
            System.out.printf("Aggregation & show time (ms) = %d%n", (tAggEnd - tAggStart) / 1_000_000);

            printJvmMemory("After aggregation");

            // Print executor info summary (local mode driver appears as executor)
            System.out.println("Executor infos: " + Arrays.toString(spark.sparkContext().statusTracker().getExecutorInfos()));

            // ---------------- Write aggregated result to Parquet warehouse folder ----------------
            if (writeToParquet) {
                long tWriteStart = System.nanoTime();
                // Partition by trade_date_str to make files manageable and to illustrate partition pruning
                aggregated.write()
                        .mode(SaveMode.Overwrite)
                        .partitionBy("trade_date_str")
                        .parquet(warehousePath);
                long tWriteEnd = System.nanoTime();
                System.out.printf("Parquet write time (ms) = %d%n", (tWriteEnd - tWriteStart) / 1_000_000);

                System.out.println("Wrote aggregated parquet to: " + warehousePath);
            }

            System.out.println("Spark memory-management demo completed successfully.");

        } catch (Exception e) {
            System.err.println("Spark job failed: " + e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            try {
                spark.stop();
            } catch (Exception ignored) {}
        }

        long globalEnd = System.nanoTime();
        System.out.println("-------------------------------------------------");
        System.out.println("Total run time (ms): " + ((globalEnd - globalStart) / 1_000_000));
        System.out.println("-------------------------------------------------");
    }

    private static void printJvmMemory(String phase) {
        Runtime rt = Runtime.getRuntime();
        long total = rt.totalMemory();
        long free = rt.freeMemory();
        long used = total - free;
        long max = rt.maxMemory();
        System.out.printf("=== JVM Memory (%s) === Used=%dMB Free=%dMB Total=%dMB Max=%dMB%n",
                phase, used / (1024 * 1024), free / (1024 * 1024),
                total / (1024 * 1024), max / (1024 * 1024));
    }
}
