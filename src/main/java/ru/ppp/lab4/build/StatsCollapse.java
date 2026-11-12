package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.sum;

/**
 * Свёртка stats до одной строки на (player_id, season): TOT побеждает строки команд,
 * иначе меры суммируются по командам. Складывать TOT и команды нельзя — двойной учёт.
 */
public final class StatsCollapse {

    private StatsCollapse() {
    }

    public static Dataset<Row> collapse(Dataset<Row> named) {
        Dataset<Row> tot = named.filter(col("tm").equalTo("TOT"));
        Dataset<Row> totKeys = tot.select(
                col("player_id").as("tot_player_id"),
                col("season").as("tot_season"));

        Dataset<Row> team = named
                .filter(col("tm").isNull().or(not(col("tm").equalTo("TOT"))))
                .join(
                        broadcast(totKeys),
                        col("player_id").equalTo(col("tot_player_id"))
                                .and(col("season").equalTo(col("tot_season"))),
                        "left_anti");

        return tot.unionByName(team)
                .groupBy("player_id", "season")
                .agg(
                        first("player_name").as("player_name"),
                        sum("g").as("games"),
                        sum("pts").as("pts"),
                        sum("trb").as("trb"),
                        sum("ast").as("ast"));
    }
}
