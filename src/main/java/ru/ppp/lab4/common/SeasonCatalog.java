package ru.ppp.lab4.common;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Каталог сезонов строится по файловой системе: season=YYYY и хотя бы один parquet-файл. */
public final class SeasonCatalog {

    private static final Pattern SEASON_DIR = Pattern.compile("season=(\\d{4})");

    private SeasonCatalog() {
    }

    public static List<Integer> scan(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            return null;
        }
        List<Integer> seasons = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(root)) {
            for (Path entry : entries) {
                if (!Files.isDirectory(entry)) {
                    continue;
                }
                Matcher matcher = SEASON_DIR.matcher(entry.getFileName().toString());
                if (!matcher.matches() || !hasParquet(entry)) {
                    continue;
                }
                seasons.add(Integer.parseInt(matcher.group(1)));
            }
        }
        Collections.sort(seasons);
        return seasons;
    }

    private static boolean hasParquet(Path dir) throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*.parquet")) {
            for (Path file : files) {
                if (Files.isRegularFile(file)) {
                    return true;
                }
            }
        }
        return false;
    }
}
