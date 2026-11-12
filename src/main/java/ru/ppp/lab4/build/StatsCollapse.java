package ru.ppp.lab4.build;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * Свёртка stats до одной строки на (player_id, season). Если для пары есть Tm = "TOT",
 * берутся только TOT-строки; иначе суммируются строки команд. Условная агрегация
 * не требует промежуточного union и отдельных TOT-ключей.
 */
public final class StatsCollapse {

    private static final String TOT = "TOT";

    private StatsCollapse() {
    }

    public static Dataset<Row> collapse(Dataset<Row> named) {
        Column isTot = col("tm").equalTo(TOT);
        Dataset<Row> aggregated = named
                .groupBy("player_id", "season")
                .agg(
                        first("player_name").as("player_name"),
                        max(when(isTot, lit(1)).otherwise(lit(0))).as("has_tot"),
                        sum(when(isTot, col("g"))).as("tot_g"),
                        sum(teamValue(isTot, "g")).as("team_g"),
                        sum(when(isTot, col("pts"))).as("tot_pts"),
                        sum(teamValue(isTot, "pts")).as("team_pts"),
                        sum(when(isTot, col("trb"))).as("tot_trb"),
                        sum(teamValue(isTot, "trb")).as("team_trb"),
                        sum(when(isTot, col("ast"))).as("tot_ast"),
                        sum(teamValue(isTot, "ast")).as("team_ast"));

        return aggregated
                .withColumn("games", choose(col("has_tot"), "tot_g", "team_g"))
                .withColumn("pts", choose(col("has_tot"), "tot_pts", "team_pts"))
                .withColumn("trb", choose(col("has_tot"), "tot_trb", "team_trb"))
                .withColumn("ast", choose(col("has_tot"), "tot_ast", "team_ast"))
                .select("player_id", "season", "player_name", "games", "pts", "trb", "ast");
    }

    /** Строка команды: TOT-строки в сумму команд не попадают. */
    private static Column teamValue(Column isTot, String name) {
        return when(isTot, lit(null).cast("long")).otherwise(col(name));
    }

    /** TOT побеждает, если он есть; иначе берётся сумма по командам. */
    private static Column choose(Column hasTot, String totName, String teamName) {
        return when(hasTot.equalTo(1), col(totName)).otherwise(col(teamName));
    }
}
