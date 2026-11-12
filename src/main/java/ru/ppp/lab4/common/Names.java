package ru.ppp.lab4.common;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.trim;

/** Одна реализация нормализации имени для players.name и Seasons_Stats.Player. */
public final class Names {

    private Names() {
    }

    public static Column normalizeName(Column column) {
        return trim(regexp_replace(
                regexp_replace(
                        regexp_replace(lower(trim(column)), "\\*", " "),
                        "[^a-z0-9 ]+", " "),
                " +", " "));
    }
}
