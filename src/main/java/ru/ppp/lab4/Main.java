package ru.ppp.lab4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.ppp.lab4.build.BuildJob;
import ru.ppp.lab4.common.ProjectPaths;
import ru.ppp.lab4.common.SeasonCatalog;
import ru.ppp.lab4.common.Spark;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Явный сценарий без CLI: собрать витрину из трёх CSV и выполнить оба запроса ТЗ
 * для фиксированного сезона 1991. Пути и параметры запросов заданы константами.
 */
public final class Main {

    private static final int SEASON = 1991;
    private static final int MIN_GAMES = 20;
    private static final int TOP_LIMIT = 5;
    private static final int UNDERVALUED_LIMIT = 10;

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
            dataset.createOrReplaceTempView("player_season_efficiency");

            printTop(spark);
            printUndervalued(spark);
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
    private static void printTop(SparkSession spark) {
        String sql = "SELECT ROW_NUMBER() OVER (ORDER BY efficiency DESC, player_id ASC) AS `rank`,"
                + " player_id, player_name, season, games, salary, pts, trb, ast, efficiency"
                + " FROM player_season_efficiency"
                + " WHERE season = " + SEASON + " AND games >= " + MIN_GAMES
                + " ORDER BY efficiency DESC, player_id ASC"
                + " LIMIT " + TOP_LIMIT;
        System.out.println("Топ " + TOP_LIMIT + " по эффективности, сезон " + SEASON
                + " (games >= " + MIN_GAMES + ")");
        printRows(spark.sql(sql).collectAsList(), TOP_LIMIT);
        System.out.println();
    }

    /** ТЗ §7.3: топ недооценённых, при равенстве — player_id ASC. */
    private static void printUndervalued(SparkSession spark) {
        String sql = "SELECT ROW_NUMBER() OVER (ORDER BY efficiency ASC, player_id ASC) AS `rank`,"
                + " player_id, player_name, season, games, salary, pts, trb, ast, efficiency"
                + " FROM player_season_efficiency"
                + " WHERE season = " + SEASON + " AND games >= " + MIN_GAMES
                + " ORDER BY efficiency ASC, player_id ASC"
                + " LIMIT " + UNDERVALUED_LIMIT;
        System.out.println("Топ " + UNDERVALUED_LIMIT + " недооценённых, сезон " + SEASON
                + " (games >= " + MIN_GAMES + ")");
        printRows(spark.sql(sql).collectAsList(), UNDERVALUED_LIMIT);
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
