package ru.ppp.lab4.build;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Счётчики раздела 9 ТЗ; печатаются до записи датасета. */
public final class BuildReport {

    public record BlacklistEntry(String nameNorm, List<String> ids, long statsRows) {
    }

    public long playersRows;
    public long playersAfterClean;
    public long playersUniqueIds;
    public long playerNames;
    public long mappingNames;
    public long blacklistNames;
    public long blacklistIds;

    public long statsRows;
    public long statsEmptyDropped;
    public long statsAfterEmpty;
    public long statsAfterPeriod;
    public long statsNamed;
    public long statsNotFoundRows;
    public long statsNotFoundNames;
    public long statsBlacklistRows;
    public long statsBlacklistNames;
    public long statsKeys;

    public long salariesRows;
    public long salariesAfterClean;
    public long salaryKeys;

    public long joinedRows;
    public long noSalary;
    public long noStats;
    public long dupes;

    public long nullTrbReplaced;
    public long nullAstReplaced;
    public long droppedNoGamesOrPts;

    public long productionDropped;
    public long resultRows;
    public long resultSeasons;

    public final List<BlacklistEntry> blacklistEntries = new ArrayList<>();

    public void print() {
        System.out.printf(Locale.ROOT,
                "[players]  rows=%d (unique _id=%d); after_clean=%d; names=%d; mapping=%d; blacklist=%d names / %d ids%n",
                playersRows, playersUniqueIds, playersAfterClean, playerNames,
                mappingNames, blacklistNames, blacklistIds);
        System.out.printf(Locale.ROOT,
                "[stats]    rows=%d -> %d (empty) -> %d (period) -> %d (named) -> %d keys%n",
                statsRows, statsAfterEmpty, statsAfterPeriod, statsNamed, statsKeys);
        System.out.printf(Locale.ROOT,
                "[named]    matched_rows=%d; not_found=%d rows / %d names; blacklist=%d rows / %d names%n",
                statsNamed, statsNotFoundRows, statsNotFoundNames,
                statsBlacklistRows, statsBlacklistNames);
        for (BlacklistEntry entry : blacklistEntries) {
            System.out.printf(Locale.ROOT, "[blacklist] name_norm=%s; ids=%s; stats_rows=%d%n",
                    entry.nameNorm(), entry.ids(), entry.statsRows());
        }
        System.out.printf(Locale.ROOT,
                "[salaries] rows=%d -> %d (period) -> %d keys%n",
                salariesRows, salariesAfterClean, salaryKeys);
        System.out.printf(Locale.ROOT,
                "[join]     stats_keys=%d; salary_keys=%d; joined=%d; no_salary=%d; no_stats=%d; dupes=%d%n",
                statsKeys, salaryKeys, joinedRows, noSalary, noStats, dupes);
        System.out.printf(Locale.ROOT,
                "[cleanup]  null_trb_replaced=%d; null_ast_replaced=%d; dropped_no_g_or_pts=%d%n",
                nullTrbReplaced, nullAstReplaced, droppedNoGamesOrPts);
        System.out.printf(Locale.ROOT, "[metric]   production<=0 dropped=%d%n", productionDropped);
        System.out.printf(Locale.ROOT, "[result]   rows=%d; seasons=%d%n",
                resultRows, resultSeasons);
    }
}
