package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Чтение трёх CSV: header=true без inferSchema, проекция нужных колонок и явные cast.
 * Навязывать частичную StructType нельзя: Spark CSV маппит схему по позиции, а не по имени.
 */
public final class Readers {

    private Readers() {
    }

    public static Dataset<Row> players(SparkSession spark, String path) {
        return spark.read().option("header", "true").csv(path)
                .select(col("_id"), col("name"));
    }

    public static Dataset<Row> stats(SparkSession spark, String path) {
        return spark.read().option("header", "true").csv(path)
                .select(
                        col("Player"),
                        col("Year").cast("int").as("season"),
                        col("Tm").as("tm"),
                        col("G").cast("double").cast("long").as("g"),
                        col("PTS").cast("double").cast("long").as("pts"),
                        col("TRB").cast("double").cast("long").as("trb"),
                        col("AST").cast("double").cast("long").as("ast"));
    }

    public static Dataset<Row> salaries(SparkSession spark, String path) {
        return spark.read().option("header", "true").csv(path)
                .select(
                        col("player_id"),
                        col("season_end").cast("int").as("season_end"),
                        col("salary").cast("double").cast("long").as("salary"));
    }
}
