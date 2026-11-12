package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.ppp.lab4.common.Names;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.first;

/**
 * name_norm -> player_id только для однозначных имён. Имена с несколькими _id
 * целиком исключаются: чёрный список как отдельный датасет больше не нужен.
 */
public final class NameRegistry {

    public final Dataset<Row> nameMap;

    private NameRegistry(Dataset<Row> nameMap) {
        this.nameMap = nameMap;
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
                        first("player_name").as("player_name"));

        Dataset<Row> nameMap = registry
                .filter(col("id_count").equalTo(1))
                .select("name_norm", "player_id", "player_name");
        return new NameRegistry(nameMap);
    }
}
