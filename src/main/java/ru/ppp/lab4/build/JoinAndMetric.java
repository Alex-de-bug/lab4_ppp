package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

/** J2 (inner), контрольный assert уникальности и вычисление метрики. */
public final class JoinAndMetric {

    private JoinAndMetric() {
    }

    public static Dataset<Row> join(Dataset<Row> stats, Dataset<Row> salaries) {
        return stats.as("s")
                .join(
                        broadcast(salaries.as("l")),
                        col("s.player_id").equalTo(col("l.player_id"))
                                .and(col("s.season").equalTo(col("l.season_end"))),
                        "inner")
                .select(
                        col("s.player_id"),
                        col("s.season"),
                        col("s.player_name"),
                        col("s.games"),
                        col("l.salary"),
                        col("s.pts"),
                        col("s.trb"),
                        col("s.ast"));
    }

    /** Возвращает максимальное число строк на (player_id, season); 1 — всё в порядке. */
    public static long maxRowsPerKey(Dataset<Row> joined) {
        Row row = joined.groupBy("player_id", "season")
                .count()
                .agg(max("count"))
                .head();
        Object value = row.get(0);
        return value == null ? 0 : ((Number) value).longValue();
    }

    public static Dataset<Row> metric(Dataset<Row> joined) {
        return joined
                .withColumn("production",
                        col("pts").plus(col("trb")).plus(col("ast")))
                .filter(col("production").gt(0))
                .withColumn("efficiency",
                        col("salary").cast("double").divide(col("production").cast("double")))
                .select(
                        col("season"),
                        col("player_id"),
                        col("player_name"),
                        col("games").cast("int").as("games"),
                        col("salary").cast("long").as("salary"),
                        col("pts").cast("int").as("pts"),
                        col("trb").cast("int").as("trb"),
                        col("ast").cast("int").as("ast"),
                        col("efficiency"));
    }

    public static void assertSchema(Dataset<Row> result) {
        StructType actual = result.schema();
        String[] expectedNames = {
                "season", "player_id", "player_name", "games", "salary",
                "pts", "trb", "ast", "efficiency",
        };
        DataType[] expectedTypes = {
                DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType,
                DataTypes.IntegerType, DataTypes.LongType, DataTypes.IntegerType,
                DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.DoubleType,
        };
        StructField[] fields = actual.fields();
        if (fields.length != expectedNames.length) {
            throw new IllegalStateException("неожиданная схема результата: " + actual.treeString());
        }
        for (int i = 0; i < expectedNames.length; i++) {
            if (!fields[i].name().equals(expectedNames[i])
                    || !fields[i].dataType().equals(expectedTypes[i])) {
                throw new IllegalStateException("неожиданная схема результата: " + actual.treeString());
            }
        }
    }
}
