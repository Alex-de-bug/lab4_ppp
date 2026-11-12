package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public final class PartitionWriter {

    private PartitionWriter() {
    }

    public static void write(Dataset<Row> result, String output) {
        result.repartition(col("season"))
                .sortWithinPartitions(
                        col("season").asc(),
                        col("efficiency").desc(),
                        col("player_id").asc())
                .write()
                .mode("overwrite")
                .partitionBy("season")
                .parquet(output);
    }
}
