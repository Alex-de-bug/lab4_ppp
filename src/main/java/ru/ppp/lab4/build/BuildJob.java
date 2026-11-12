package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.ppp.lab4.common.Args;
import ru.ppp.lab4.common.Names;
import ru.ppp.lab4.common.Spark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Set;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.trim;

/** Сборка: очистка каждого источника, J1 по имени, J2 с зарплатами, метрика и Parquet. */
public final class BuildJob {

    public int run(String[] argv) {
        Args args;
        try {
            args = Args.parse(argv,
                    Set.of("stats", "players", "salaries", "output"),
                    Set.of());
        } catch (Args.ArgException e) {
            System.err.println(e.getMessage());
            return 2;
        }

        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path statsPath = resolve(cwd, args.getOr("stats", "Seasons_Stats.xls"));
        Path playersPath = resolve(cwd, args.getOr("players", "players.csv"));
        Path salariesPath = resolve(cwd, args.getOr("salaries", "salaries_1985to2018.csv"));
        Path output = resolve(cwd, args.getOr("output", "data/player_season_efficiency"));

        for (Path input : new Path[]{statsPath, playersPath, salariesPath}) {
            if (!Files.isRegularFile(input)) {
                System.err.println("файл не найден: " + input);
                return 1;
            }
        }

        SparkSession spark = null;
        try {
            spark = Spark.session("nba-efficiency-build");
            build(spark, statsPath, playersPath, salariesPath, output);
            return 0;
        } catch (Exception e) {
            System.err.println("сборка не удалась: " + e.getMessage());
            e.printStackTrace(System.err);
            return 1;
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    private static void build(SparkSession spark, Path statsPath, Path playersPath,
                              Path salariesPath, Path output) {
        BuildReport report = new BuildReport();

        // P1: players -> name_map / blacklist.
        Dataset<Row> playersRaw = Readers.players(spark, playersPath.toString());
        report.playersRows = playersRaw.count();
        Dataset<Row> playersClean = playersRaw
                .filter(col("_id").isNotNull()
                        .and(length(trim(col("_id"))).gt(0))
                        .and(col("name").isNotNull())
                        .and(length(trim(col("name"))).gt(0)))
                .cache();
        report.playersAfterClean = playersClean.count();
        report.playersUniqueIds = playersClean.select("_id").distinct().count();
        report.playerNames = playersClean
                .select(Names.normalizeName(col("name")).as("name_norm"))
                .distinct()
                .count();

        NameRegistry registry = NameRegistry.build(playersClean);
        registry.nameMap.cache();
        registry.blacklist.cache();
        report.mappingNames = registry.nameMap.count();
        report.blacklistNames = registry.blacklist.count();
        Row blacklistSizes = registry.blacklist
                .agg(sum(size(col("ids"))).as("n"))
                .head();
        report.blacklistIds = blacklistSizes.isNullAt(0) ? 0 : blacklistSizes.getLong(0);

        // P2: stats -> очистка -> период -> name_norm.
        Dataset<Row> statsRaw = Readers.stats(spark, statsPath.toString());
        report.statsRows = statsRaw.count();
        Dataset<Row> statsClean = statsRaw
                .filter(col("Player").isNotNull()
                        .and(length(trim(col("Player"))).gt(0))
                        .and(col("season").isNotNull()))
                .cache();
        report.statsAfterEmpty = statsClean.count();
        report.statsEmptyDropped = report.statsRows - report.statsAfterEmpty;

        Dataset<Row> statsPeriod = statsClean
                .filter(col("season").between(1985, 2017))
                .cache();
        report.statsAfterPeriod = statsPeriod.count();
        Dataset<Row> normalized = statsPeriod
                .withColumn("name_norm", Names.normalizeName(col("Player")))
                .cache();

        Dataset<Row> blacklistStats = normalized.join(
                broadcast(registry.blacklist.select(col("name_norm").as("bl_name"))),
                col("name_norm").equalTo(col("bl_name")),
                "inner");
        report.statsBlacklistRows = blacklistStats.count();
        report.statsBlacklistNames = blacklistStats.select("name_norm").distinct().count();

        Dataset<Row> named = normalized.join(broadcast(registry.nameMap), "name_norm").cache();
        report.statsNamed = named.count();

        Dataset<Row> notFoundNames = normalized
                .select(col("name_norm").as("nf_name"))
                .distinct()
                .join(broadcast(registry.nameMap.select(col("name_norm").as("map_name"))),
                        col("nf_name").equalTo(col("map_name")),
                        "left_anti")
                .join(broadcast(registry.blacklist.select(col("name_norm").as("bl_name"))),
                        col("nf_name").equalTo(col("bl_name")),
                        "left_anti");
        report.statsNotFoundNames = notFoundNames.count();
        report.statsNotFoundRows = normalized
                .join(notFoundNames, col("name_norm").equalTo(col("nf_name")), "inner")
                .count();

        Dataset<Row> statsCounts = normalized.groupBy("name_norm").count();
        for (Row row : registry.blacklist
                .join(statsCounts, "name_norm", "left")
                .select("name_norm", "ids", "count")
                .orderBy("name_norm")
                .collectAsList()) {
            long rows = row.isNullAt(2) ? 0 : row.getLong(2);
            report.blacklistEntries.add(
                    new BuildReport.BlacklistEntry(row.getString(0), row.getList(1), rows));
        }

        // P3: TOT/team collapse, фильтр G/PTS, null TRB/AST -> 0.
        Dataset<Row> collapsed = StatsCollapse.collapse(named).cache();
        long collapsedRows = collapsed.count();
        Dataset<Row> valid = collapsed
                .filter(col("games").isNotNull().and(col("pts").isNotNull()));
        report.droppedNoGamesOrPts = collapsedRows - valid.count();
        report.nullTrbReplaced = valid.filter(col("trb").isNull()).count();
        report.nullAstReplaced = valid.filter(col("ast").isNull()).count();
        Dataset<Row> stats = valid
                .withColumn("trb", coalesce(col("trb"), lit(0L)))
                .withColumn("ast", coalesce(col("ast"), lit(0L)))
                .cache();
        report.statsKeys = stats.count();

        // P4/P5: salaries.
        Dataset<Row> salariesRaw = Readers.salaries(spark, salariesPath.toString());
        report.salariesRows = salariesRaw.count();
        Dataset<Row> salariesClean = salariesRaw
                .filter(col("player_id").isNotNull()
                        .and(length(trim(col("player_id"))).gt(0))
                        .and(col("season_end").isNotNull())
                        .and(col("salary").isNotNull())
                        .and(col("salary").gt(0))
                        .and(col("season_end").between(1985, 2017)))
                .cache();
        report.salariesAfterClean = salariesClean.count();
        Dataset<Row> salaries = SalaryCollapse.collapse(salariesClean).cache();
        report.salaryKeys = salaries.count();

        // P6: J2 + uniqueness assert + метрика.
        Dataset<Row> joined = JoinAndMetric.join(stats, salaries).cache();
        report.joinedRows = joined.count();
        long maxPerKey = JoinAndMetric.maxRowsPerKey(joined);
        report.dupes = maxPerKey > 1 ? 1 : 0;
        if (maxPerKey > 1) {
            throw new IllegalStateException(
                    "duplicate (player_id, season) after join, max=" + maxPerKey);
        }
        report.noSalary = report.statsKeys - report.joinedRows;
        report.noStats = report.salaryKeys - report.joinedRows;

        Dataset<Row> withProduction = joined.withColumn("production",
                col("pts").plus(col("trb")).plus(col("ast")));
        report.productionDropped = withProduction.filter(col("production").leq(0)).count();

        Dataset<Row> result = JoinAndMetric.metric(joined).cache();
        report.resultRows = result.count();
        report.resultSeasons = result.select("season").distinct().count();
        JoinAndMetric.assertSchema(result);

        report.print();

        // P7: запись.
        PartitionWriter.write(result, output.toString());
        System.out.printf(Locale.ROOT, "[write]     %s%n", output);
    }

    private static Path resolve(Path cwd, String value) {
        Path path = Paths.get(value);
        return (path.isAbsolute() ? path : cwd.resolve(path)).normalize();
    }
}
