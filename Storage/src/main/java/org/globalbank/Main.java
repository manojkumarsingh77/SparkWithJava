package org.globalbank;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {

        // ----------------------------
        // CONFIG - ADLS read settings
        // ----------------------------
        final String storageAccount = System.getenv().getOrDefault("ADLS_ACCOUNT", "tickdatasocgen");
        final String containerRaw = System.getenv().getOrDefault("ADLS_CONTAINER_RAW", "fx-raw");

        // Provide ADLS account key here or via ADLS_KEY env var
        final String accountKey = System.getenv().getOrDefault("ADLS_KEY",
                // If you previously hard-coded the key, replace the string below or set
                // ADLS_KEY env var.
                "Add ADLS key here");

        final String accountFQDN = storageAccount + ".dfs.core.windows.net";
        final String rawAbfss = "abfss://" + containerRaw + "@" + accountFQDN + "/";

        // ----------------------------
        // LOCAL write settings
        // ----------------------------
        final String localProcessedDir = System.getProperty("user.dir") + "/data/processed_local/";

        System.out.println("Starting FX Tick Processor (ADLS -> Local Parquet)");
        System.out.println("ADLS raw path   : " + rawAbfss);
        System.out.println("Local processed : " + localProcessedDir);

        // Ensure local output directory exists
        try {
            Files.createDirectories(Path.of(localProcessedDir));
        } catch (IOException e) {
            System.err.println("Failed to create local output directory: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        // ----------------------------
        // Spark session
        // ----------------------------
        SparkSession spark = SparkSession.builder()
                .appName("FX Tick Processor - ADLS to Local")
                .master("local[*]")
                // set ADLS account key so ABFS can authenticate when reading
                .config("spark.hadoop.fs.azure.account.key." + accountFQDN, accountKey)
                // explicit ABFS implementation
                .config("spark.hadoop.fs.azure.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
                .getOrCreate();

        // reduce noisy logs during debugging if you want
        spark.sparkContext().setLogLevel("INFO");

        try {
            // Read NDJSON from ADLS using glob
            String glob = rawAbfss + "year=*/month=*/day=*/**/*.ndjson";
            System.out.println("Reading raw files from: " + glob);

            Dataset<Row> raw = spark.read()
                    .option("multiLine", false)
                    .json(glob);

            long rawCount = raw.count();
            System.out.println("Raw count: " + rawCount);
            if (rawCount == 0L) {
                System.out.println("No raw files found at: " + glob + " — exiting.");
                return;
            }

            System.out.println("RAW SAMPLE:");
            raw.show(10, false);

            // Transformations: parse, cast, filter, dedupe, enrich
            Dataset<Row> parsed = raw
                    .withColumn("trade_ts_parsed", to_timestamp(col("trade_ts")))
                    .withColumn("recv_ts_parsed", to_timestamp(col("recv_ts")))
                    .withColumn("bid", col("bid").cast(DataTypes.DoubleType))
                    .withColumn("ask", col("ask").cast(DataTypes.DoubleType))
                    .withColumn("ingest_ts", current_timestamp())
                    .filter(col("bid").gt(0)
                            .and(col("ask").gt(0))
                            .and(col("ask").gt(col("bid"))));

            WindowSpec w = Window.partitionBy(col("sequence_id")).orderBy(col("recv_ts_parsed").desc());
            Dataset<Row> deduped = parsed.withColumn("rn", row_number().over(w)).filter(col("rn").equalTo(1))
                    .drop("rn");

            Dataset<Row> enriched = deduped
                    .withColumn("mid_price", expr("(bid + ask) / 2"))
                    .withColumn("year", year(col("trade_ts_parsed")))
                    .withColumn("month", date_format(col("trade_ts_parsed"), "MM"))
                    .withColumn("day", date_format(col("trade_ts_parsed"), "dd"))
                    .withColumn("pair_clean", regexp_replace(col("pair"), "/", "_"));

            System.out.println("ENRICHED SAMPLE:");
            enriched.show(20, false);

            long totalCount = enriched.count();
            System.out.println("Total rows to write: " + totalCount);

            if (totalCount > 0) {
                // Write partitioned Parquet to local directory
                enriched.repartition(col("year"), col("month"), col("day"), col("pair_clean"))
                        .write()
                        .mode(SaveMode.Overwrite)
                        .option("compression", "snappy")
                        .partitionBy("year", "month", "day", "pair_clean")
                        .parquet(localProcessedDir);

                System.out.println("Wrote partitioned Parquet to local path: " + localProcessedDir);

                // Additionally write a single-file Parquet for quick inspection
                String singleFileDir = localProcessedDir + "singlefile_debug/";
                enriched.coalesce(1)
                        .write()
                        .mode(SaveMode.Overwrite)
                        .option("compression", "snappy")
                        .parquet(singleFileDir);

                System.out.println("Wrote single-file Parquet (coalesced) to: " + singleFileDir);

                // List local files written
                try (Stream<Path> walker = Files.walk(Path.of(localProcessedDir))) {
                    System.out.println("Local files under " + localProcessedDir + ":");
                    walker.filter(Files::isRegularFile)
                            .map(Path::toString)
                            .forEach(p -> System.out.println("  " + p));
                } catch (IOException ioe) {
                    System.err.println("Failed to list local processed files: " + ioe.getMessage());
                }

                // Also list files under singleFileDir
                try (Stream<Path> walker = Files.walk(Path.of(singleFileDir))) {
                    System.out.println("Files under single-file debug dir " + singleFileDir + ":");
                    walker.filter(Files::isRegularFile)
                            .map(Path::toString)
                            .forEach(p -> System.out.println("  " + p));
                } catch (IOException ioe) {
                    System.err.println("Failed to list single-file debug files: " + ioe.getMessage());
                }

            } else {
                System.out.println("No rows to write after filtering/enrichment.");
            }

        } catch (Exception ex) {
            System.err.println("Processing failed: " + ex.getMessage());
            ex.printStackTrace(System.err);
        } finally {
            spark.stop();
            System.out.println("Spark session stopped.");
        }
    }
}
