package ru.ppp.lab4.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/** Свёртка зарплат: трейд разбивает выплату на строки, полная зарплата — их сумма. */
public final class SalaryCollapse {

    private SalaryCollapse() {
    }

    public static Dataset<Row> collapse(Dataset<Row> cleaned) {
        return cleaned
                .groupBy("player_id", "season_end")
                .agg(sum(col("salary")).as("salary"));
    }
}
