package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * repartition(season) гарантирует, что все строки сезона пишет одна задача.
 * season идёт первым в сортировке: Spark при partitionBy сам требует порядок по
 * partition-колонке и иначе добавляет собственный Sort, теряя порядок efficiency.
 * Внутри каждой партиции остаётся efficiency DESC, player_id ASC.
 */
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
