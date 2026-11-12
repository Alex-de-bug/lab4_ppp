package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.ppp.lab4.common.Names;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.sort_array;

/**
 * D1/D2: name_norm -> player_id для однозначных имён; имена с несколькими _id целиком
 * уходят в blacklist. Повторные строки одного _id неоднозначности не создают.
 */
public final class NameRegistry {

    public final Dataset<Row> nameMap;
    public final Dataset<Row> blacklist;

    private NameRegistry(Dataset<Row> nameMap, Dataset<Row> blacklist) {
        this.nameMap = nameMap;
        this.blacklist = blacklist;
    }

    public static NameRegistry build(Dataset<Row> cleanedPlayers) {
        Dataset<Row> registry = cleanedPlayers
                .select(
                        col("_id").as("player_id"),
                        col("name").as("player_name"),
                        Names.normalizeName(col("name")).as("name_norm"))
                .groupBy("name_norm")
                .agg(
                        countDistinct("player_id").as("id_count"),
                        first("player_id").as("player_id"),
                        first("player_name").as("player_name"),
                        sort_array(collect_set("player_id")).as("ids"));

        Dataset<Row> nameMap = registry
                .filter(col("id_count").equalTo(1))
                .select("name_norm", "player_id", "player_name");
        Dataset<Row> blacklist = registry
                .filter(col("id_count").gt(1))
                .select("name_norm", "ids");
        return new NameRegistry(nameMap, blacklist);
    }
}
