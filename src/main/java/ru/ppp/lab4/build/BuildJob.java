package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.ppp.lab4.common.Names;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.trim;

/** Сборка: очистка каждого источника, J1 по имени, J2 с зарплатами, метрика и Parquet. */
public final class BuildJob {

    public static void build(SparkSession spark, Path statsPath, Path playersPath,
                             Path salariesPath, Path output) {
        for (Path input : new Path[]{statsPath, playersPath, salariesPath}) {
            if (!Files.isRegularFile(input)) {
                throw new IllegalArgumentException("файл не найден: " + input);
            }
        }

        // P1: players -> однозначный mapping name_norm -> player_id.
        Dataset<Row> playersClean = Readers.players(spark, playersPath.toString())
                .filter(col("_id").isNotNull()
                        .and(length(trim(col("_id"))).gt(0))
                        .and(col("name").isNotNull())
                        .and(length(trim(col("name"))).gt(0)));
        NameRegistry registry = NameRegistry.build(playersClean);

        // P2: stats -> очистка -> период -> name_norm -> J1.
        Dataset<Row> normalized = Readers.stats(spark, statsPath.toString())
                .filter(col("Player").isNotNull()
                        .and(length(trim(col("Player"))).gt(0))
                        .and(col("season").isNotNull()))
                .withColumn("name_norm", Names.normalizeName(col("Player")));
        Dataset<Row> named = normalized.join(broadcast(registry.nameMap), "name_norm");

        // P3: TOT/team collapse, фильтр G/PTS.
        Dataset<Row> stats = StatsCollapse.collapse(named)
                .filter(col("games").isNotNull().and(col("pts").isNotNull()));

        // P4/P5: salaries -> очистка -> sum(salary) по (player_id, season_end).
        Dataset<Row> salaries = SalaryCollapse.collapse(
                Readers.salaries(spark, salariesPath.toString())
                        .filter(col("player_id").isNotNull()
                                .and(length(trim(col("player_id"))).gt(0))
                                .and(col("season_end").isNotNull())
                                .and(col("salary").isNotNull())
                                .and(col("salary").gt(0))));

        // P6: J2 + uniqueness assert + метрика.
        Dataset<Row> joined = JoinAndMetric.join(stats, salaries).cache();
        long maxPerKey = JoinAndMetric.maxRowsPerKey(joined);
        if (maxPerKey > 1) {
            throw new IllegalStateException(
                    "duplicate (player_id, season) after join, max=" + maxPerKey);
        }
        Dataset<Row> result = JoinAndMetric.metric(joined);
        JoinAndMetric.assertSchema(result);

        // P7: запись.
        PartitionWriter.write(result, output.toString());
        System.out.printf(Locale.ROOT, "[write]     %s%n", output);
    }
}
