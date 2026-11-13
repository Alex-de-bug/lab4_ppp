package ru.ppp.lab4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import ru.ppp.lab4.build.BuildJob;
import ru.ppp.lab4.common.ProjectPaths;
import ru.ppp.lab4.common.SeasonCatalog;
import ru.ppp.lab4.common.Spark;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

/**
 * Явный сценарий без CLI: собрать витрину из трёх CSV и выполнить оба запроса ТЗ
 * для фиксированного сезона 1991. Пути и параметры запросов заданы константами.
 */
public final class Main {

    private static final int SEASON = 2010;
    private static final int TOP_LIMIT = 10;
    private static final int UNDERVALUED_LIMIT = 5;

    private Main() {
    }

    public static void main(String[] ignored) {
        try {
            run();
        } catch (Exception e) {
            System.err.println("ошибка: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void run() {
        Path root = ProjectPaths.projectRoot();
        Path data = root.resolve("data/player_season_efficiency");
        SparkSession spark = Spark.session("nba-efficiency");
        try {
            BuildJob.build(spark,
                    root.resolve("Seasons_Stats.xls"),
                    root.resolve("players.csv"),
                    root.resolve("salaries_1985to2018.csv"),
                    data);

            validateSeason(data);
            Dataset<Row> dataset = spark.read().parquet(data.toString());

            printTop(dataset);
            printUndervalued(dataset);
        } finally {
            spark.stop();
        }
    }

    /** Каталог сезонов строится по ФС; жёсткого диапазона 1985–2017 нет. */
    private static void validateSeason(Path data) {
        List<Integer> seasons;
        try {
            seasons = SeasonCatalog.scan(data);
        } catch (Exception e) {
            throw new IllegalStateException("не удалось прочитать каталог сезонов: " + data, e);
        }
        if (seasons == null) {
            throw new IllegalStateException("датасет не найден: " + data);
        }
        if (seasons.isEmpty()) {
            throw new IllegalStateException("датасет пуст: " + data);
        }
        if (!seasons.contains(SEASON)) {
            throw new IllegalStateException("сезон " + SEASON
                    + " отсутствует в каталоге; доступные сезоны: "
                    + seasons.stream().map(String::valueOf).collect(Collectors.joining(", ")));
        }
    }

    /** ТЗ §7.2: топ по большей эффективности, при равенстве — player_id ASC. */
    private static void printTop(Dataset<Row> dataset) {
        WindowSpec window = Window.partitionBy(col("season"))
                .orderBy(col("efficiency").desc(), col("player_id").asc());
        Dataset<Row> result = dataset
                .filter(col("season").equalTo(SEASON))
                .withColumn("rank", row_number().over(window))
                .select(col("rank"), col("player_id"), col("player_name"), col("season"),
                        col("games"), col("salary"), col("pts"), col("trb"), col("ast"),
                        col("efficiency"))
                .orderBy(col("efficiency").desc(), col("player_id").asc())
                .limit(TOP_LIMIT);
        System.out.println("Топ " + TOP_LIMIT + " по эффективности, сезон " + SEASON);
        printRows(result.collectAsList(), TOP_LIMIT);
        System.out.println();
    }

    /** ТЗ §7.3: топ недооценённых, при равенстве — player_id ASC. */
    private static void printUndervalued(Dataset<Row> dataset) {
        WindowSpec window = Window.partitionBy(col("season"))
                .orderBy(col("efficiency").asc(), col("player_id").asc());
        Dataset<Row> result = dataset
                .filter(col("season").equalTo(SEASON))
                .withColumn("rank", row_number().over(window))
                .select(col("rank"), col("player_id"), col("player_name"), col("season"),
                        col("games"), col("salary"), col("pts"), col("trb"), col("ast"),
                        col("efficiency"))
                .orderBy(col("efficiency").asc(), col("player_id").asc())
                .limit(UNDERVALUED_LIMIT);
        System.out.println("Топ " + UNDERVALUED_LIMIT + " недооценённых, сезон " + SEASON);
        result.show(UNDERVALUED_LIMIT, false);
    }

    private static void printRows(List<Row> rows, int requested) {
        System.out.println("rank\tplayer_id\tplayer_name\tseason\tgames\tsalary\tpts\ttrb\tast\tefficiency");
        for (Row row : rows) {
            System.out.printf(Locale.ROOT, "%d\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%.6f%n",
                    row.getInt(0),
                    row.getString(1),
                    row.getString(2),
                    row.getInt(3),
                    row.getInt(4),
                    row.getLong(5),
                    row.getInt(6),
                    row.getInt(7),
                    row.getInt(8),
                    row.getDouble(9));
        }
        if (rows.isEmpty()) {
            System.err.println("нет кандидатов");
        } else if (rows.size() < requested) {
            System.err.println("найдено " + rows.size() + " < " + requested + " кандидатов");
        }
    }
}
