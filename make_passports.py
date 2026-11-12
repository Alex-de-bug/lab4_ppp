#!/usr/bin/env python3
"""Генерация паспортов датасетов lab4_ppp. Запуск: python3 make_passports.py

Все числа в passports/*.md считаются из исходных CSV; вручную не правятся.
"""
import hashlib
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "passports"


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load(name):
    path = ROOT / name
    enc = "utf-8-sig" if path.open("rb").read(3) == b"\xef\xbb\xbf" else "utf-8"
    df = pd.read_csv(path, encoding=enc)
    meta = {
        "file": name,
        "size": path.stat().st_size,
        "md5": md5(path),
        "encoding": enc,
        "rows": len(df),
        "cols": df.shape[1],
    }
    return df, meta


def norm(s):
    """Имя -> ключ сравнения: без регистра, пунктуации, пробелов и метки HOF '*'."""
    return re.sub(r"[^a-z]", "", str(s).lower().replace("*", ""))


def pct(n, d):
    return f"{n / d:.1%}" if d else "—"


def short(v):
    if pd.isna(v):
        return ""
    s = str(v)
    return (s[:40] + "…") if len(s) > 41 else s


def num_fmt(v):
    if abs(v) >= 1e6:
        return f"{v:,.0f}"
    return f"{v:g}"


def col_range(s):
    nn = s.notna().sum()
    if nn == 0:
        return "—"
    if pd.api.types.is_numeric_dtype(s):
        return (
            f"{num_fmt(s.min())} … {num_fmt(s.max())} "
            f"(медиана {num_fmt(s.median())})"
        )
    vals = s.dropna().astype(str).str.strip()
    top = vals.value_counts().head(3).index.tolist()
    return "; ".join(short(v) for v in top)


def field_table(df):
    lines = [
        "| Поле | dtype | Nulls | Null % | Уникальных | Примеры / диапазон |",
        "|---|---|---:|---:|---:|---|",
    ]
    for c in df.columns:
        s = df[c]
        nulls = int(s.isna().sum())
        lines.append(
            f"| `{c}` | {s.dtype} | {nulls} | {pct(nulls, len(df))} | "
            f"{s.nunique(dropna=True)} | {col_range(s)} |"
        )
    return "\n".join(lines)


def header(meta, title, grain):
    return f"""# {title}

| | |
|---|---|
| Файл | `{meta['file']}` |
| Размер | {meta['size']:,} байт |
| Формат / кодировка | CSV / {meta['encoding']} |
| MD5 | `{meta['md5']}` |
| Строк × колонок | {meta['rows']:,} × {meta['cols']} |
| Гранулярность | {grain} |

Числа ниже посчитаны скриптом `make_passports.py` по исходному файлу.
"""


def main():
    OUT.mkdir(exist_ok=True)
    ss, ss_meta = load("Seasons_Stats.xls")
    pl, pl_meta = load("players.csv")
    sa, sa_meta = load("salaries_1985to2018.csv")

    # ---------- общие ключи ----------
    blank_ss = int(ss.Player.isna().sum())
    ss_valid = ss.dropna(subset=["Player", "Year"])

    pl = pl.assign(nn=pl.name.map(norm))
    name_counts = pl.nn.value_counts()
    ambiguous = set(name_counts[name_counts > 1].index)
    name2id = (
        pl[~pl.nn.isin(ambiguous)].drop_duplicates("nn").set_index("nn")._id
    )

    stats_names = set(ss_valid.Player.unique())
    players_names = set(pl.name.dropna().unique())
    norm_stats = {norm(x) for x in stats_names}
    norm_players = set(pl.nn)
    exact_hits = len(stats_names & players_names)
    norm_hits = len(norm_stats & norm_players)
    unmatched_names = sorted(norm_stats - norm_players)

    ss_map = ss_valid.assign(
        nn=ss_valid.Player.map(norm),
        pid=ss_valid.Player.map(norm).map(name2id),
    )
    sub = ss_map[(ss_map.Year >= 1985) & (ss_map.Year <= 2017)].copy()
    merged = sub.merge(
        sa,
        left_on=["pid", "Year"],
        right_on=["player_id", "season_end"],
        how="left",
        indicator=True,
    )
    n_sub = len(sub)
    no_pid = int(sub.pid.isna().sum())
    no_pid_ambiguous = int(sub.nn.isin(ambiguous).sum())
    mapped_no_salary = int(((merged.pid.notna()) & (merged._merge == "left_only")).sum())
    matched = int(merged.loc[merged._merge == "both", "Unnamed: 0"].nunique())
    assert matched + no_pid + mapped_no_salary == n_sub, (
        matched, no_pid, mapped_no_salary, n_sub
    )

    salary_ids = set(sa.player_id)
    player_ids = set(pl._id)
    ids_both = len(salary_ids & player_ids)
    null_teams = int(sa.team.isna().sum())

    # ---------- Seasons_Stats ----------
    tot_rows = int((ss_valid.Tm == "TOT").sum())
    dup_groups = int(ss_valid.duplicated(["Player", "Year"]).sum())
    dup_rows = int(ss_valid.duplicated(["Player", "Year"], keep=False).sum())
    tot = ss_valid[ss_valid.Tm == "TOT"].set_index(["Player", "Year"])
    team_sum = (
        ss_valid[ss_valid.Tm != "TOT"]
        .groupby(["Player", "Year"])[["G", "PTS"]]
        .sum()
    )
    tot_check = tot[["G", "PTS"]].join(
        team_sum, lsuffix="_tot", rsuffix="_sum", how="inner"
    )
    tot_eq = {
        c: ((tot_check[f"{c}_tot"] - tot_check[f"{c}_sum"]).abs() < 0.01).mean()
        for c in ["G", "PTS"]
    }
    hof_names = int(ss_valid.Player.str.contains(r"\*", regex=True).sum())

    ss_md = header(
        ss_meta,
        "Паспорт: Seasons_Stats.xls",
        "игрок × сезон × команда (строка `Tm = TOT` — агрегат по всем командам сезона)",
    )
    ss_md += f"""
## Ключи и покрытие

- Расширение `.xls` не соответствует содержимому: это обычный CSV (проверено `file`).
- Сезоны: **{ss_valid.Year.min():.0f}–{ss_valid.Year.max():.0f}**; `Year` — год **окончания** сезона
  (сезон 1990/91 → `Year = 1991`).
- Уникальных игроков: **{ss_valid.Player.nunique():,}**; кодов команд: **{ss_valid.Tm.nunique()}** (включая `TOT`).
- Пустых строк (полностью NaN, кроме индекса): **{blank_ss:,}** — удалять; служебную колонку
  `Unnamed: 0` и пустые колонки `blanl`, `blank2` при загрузке отбрасывать.
- **{tot_rows:,}** строк `Tm = "TOT"`; строк, вовлечённых в дубликаты `(Player, Year)`: **{dup_rows:,}**
  (лишних сверх первого вхождения: **{dup_groups:,}**; это норма: `TOT` + строки по командам, либо несколько команд).
- `TOT` совпадает с суммой строк команд: G — {tot_eq['G']:.1%}, PTS — {tot_eq['PTS']:.1%}
  ({len(tot_check):,} проверенных пар игрок-сезон).
- Строк с меткой `*` (HOF) в имени: `{hof_names:,}` — при join метку снимать (`norm`).

**Правило мержа:** для сезонных итогов игрока брать `Tm == "TOT"`; иначе — сумма по строкам команд.
Ключ `(Player, Year)` не уникален; ID игрока в файле отсутствует.

## Словарь полей

{field_table(ss)}
"""
    (OUT / "seasons_stats.md").write_text(ss_md, encoding="utf-8")

    # ---------- players ----------
    dup_name_rows = int(pl.duplicated("name").sum())
    dup_name_involved = int(pl.duplicated("name", keep=False).sum())
    dash_cols = [
        c for c in pl.columns
        if pl[c].dtype == object and (pl[c].dropna().astype(str) == "-").any()
    ]
    dash_text = ", ".join("`{}`".format(c) for c in dash_cols)

    pl_md = header(
        pl_meta,
        "Паспорт: players.csv",
        "один игрок (биография и карьерные итоги), 1 строка = 1 `_id`",
    )
    pl_md += f"""
## Ключи и покрытие

- `_id` уникален: **{pl._id.is_unique}**; это ID Basketball-Reference, он же `player_id` в зарплатах.
- Покрытие зарплат: **{ids_both:,} / {len(salary_ids):,}** ({pct(ids_both, len(salary_ids))}) ID из зарплат есть в players;
  зарплатные строки есть у **{ids_both:,} / {len(player_ids):,}** ({pct(ids_both, len(player_ids))}) игроков.
- Полных дублей имён: **{dup_name_rows}** строк ({dup_name_involved} задействовано); после нормализации
  (регистр, пунктуация, пробелы) **{len(ambiguous)}** неоднозначных имён — по имени таких игроков различать нельзя,
  только по `_id`.
- `career_*` — карьерные показатели смешанной семантики: `career_G`, `career_WS` — суммарные;
  `career_PTS/AST/TRB` — в среднем за игру; проценты — карьерные. Отсутствие значения: `"-"` или пустая ячейка
  (колонки с `"-"`: {dash_text}).
- `height` — строка `футы-дюймы` (`6-10`), `weight` — строка с суффиксом (`240lb`), `draft_pick` — текст (`25th overall`).

## Словарь полей

{field_table(pl.drop(columns=["nn"]))}
"""
    (OUT / "players.md").write_text(pl_md, encoding="utf-8")

    # ---------- salaries ----------
    dup_salary_rows = int(sa.duplicated(["player_id", "season"]).sum())
    dup_salary_involved = int(sa.duplicated(["player_id", "season"], keep=False).sum())
    dup_salary_players = int(
        sa[sa.duplicated(["player_id", "season"], keep=False)].player_id.nunique()
    )
    per_season = sa.groupby("season_end").agg(
        rows=("salary", "size"),
        players=("player_id", "nunique"),
        min_salary=("salary", "min"),
        median_salary=("salary", "median"),
        max_salary=("salary", "max"),
    )
    season_table = "\n".join(
        f"| {int(y)} | {int(r.rows)} | {int(r.players)} | {int(r.min_salary):,} | "
        f"{r.median_salary:,.0f} | {int(r.max_salary):,} |"
        for y, r in per_season.iterrows()
    )
    gap_years = per_season[per_season.rows < 100]
    gap_text = ", ".join("`{}` ({})".format(int(y), int(r.rows)) for y, r in gap_years.iterrows())
    gap_text_plain = ", ".join(
        "{} ({} строк)".format(int(y), int(r.rows)) for y, r in gap_years.iterrows()
    )
    gap_years_text = ", ".join(str(int(y)) for y in gap_years.index)
    league_text = ", ".join(map(str, sa.league.unique()))
    unmatched_text = ", ".join(unmatched_names[:8])

    sa_md = header(
        sa_meta,
        "Паспорт: salaries_1985to2018.csv",
        "игрок × сезон × команда (у трейдов зарплата разбита на несколько строк)",
    )
    sa_md += f"""
## Ключи и покрытие

- Сезоны: **{sa.season.min()} – {sa.season.max()}**; `season_end` = год окончания
  (`1990-91` → `season_end = 1991`, `season_start = 1990`).
- Игроков: **{sa.player_id.nunique():,}**; команд: **{sa.team.nunique()}**; лига: {league_text}.
- `(player_id, season)` не уникален: **{dup_salary_rows}** лишних строк, всего **{dup_salary_involved}** строк
  у **{dup_salary_players}** игроков — это разбивка зарплаты между командами при трейде.
  **Правило мержа:** сумма `salary` по `(player_id, season)` = полная зарплата за сезон.
- Пропуски `team`: **{null_teams:,}** строк (сезон 1984-85).
- Неполные сезоны (аномально мало строк): {gap_text}
  — рядом с соседними сезонами ~300–550 строк; вероятен пропуск данных в источнике.
- Диапазон зарплат: **{sa.salary.min():,} … {sa.salary.max():,}** (без поправки на инфляцию).

## Строк по сезонам

| season_end | строк | игроков | min | median | max |
|---:|---:|---:|---:|---:|---:|
{season_table}

## Словарь полей

{field_table(sa)}
"""
    (OUT / "salaries.md").write_text(sa_md, encoding="utf-8")

    # ---------- README ----------
    readme = f"""# Паспорта датасетов и правила объединения

Сгенерировано `make_passports.py`. Исходные файлы не изменяются.

## Сводка

| Датасет | Строк | Колонок | Гранулярность | Ключ | Период |
|---|---:|---:|---|---|---|
| `Seasons_Stats.xls` | {ss_meta['rows']:,} | {ss_meta['cols']} | игрок × сезон × команда (+`TOT`) | `(Player, Year)` не уникален, ID нет | 1950–2017 |
| `players.csv` | {pl_meta['rows']:,} | {pl_meta['cols']} | 1 игрок | `_id` (B-Ref ID) | карьера |
| `salaries_1985to2018.csv` | {sa_meta['rows']:,} | {sa_meta['cols']} | игрок × сезон × команда | `(player_id, season)` не уникален | 1985–2018 |

## Как связаны

- `players._id` = `salaries.player_id` — точное совпадение: **{ids_both:,}/{len(salary_ids):,}** ID из зарплат
  ({pct(ids_both, len(salary_ids))}); у **{pct(ids_both, len(player_ids))}** игроков из players есть зарплата.
- `Seasons_Stats` не содержит ID. Связь только через имя → `players._id`:
  - точное совпадение имён: **{exact_hits:,}/{len(stats_names):,}** ({pct(exact_hits, len(stats_names))});
  - после нормализации (регистр, пунктуация, `*`): **{norm_hits:,}/{len(norm_stats):,}** ({pct(norm_hits, len(norm_stats))});
  - неоднозначных нормализованных имён (разные игроки с одинаковым именем): **{len(ambiguous)}** — join по имени для них запрещён;
  - примеры ненайденных имён: {unmatched_text}.

## Правила объединения

1. **Канонический ключ игрока** — `players._id` (= `salaries.player_id`). Для stats сначала строить
   `Player → _id` по нормализованному имени, неоднозначные имена разбирать вручную (позиция/возраст/команда/год).
2. **Ключ сезона** — `Seasons_Stats.Year == salaries.season_end` (`season_start = Year - 1`).
   Проверено на Alaa Abdelnaby: stats 1991 ↔ salary 1990-91.
3. **Stats:** удалить {blank_ss:,} пустых строк; для итогов игрока за сезон брать `Tm == "TOT"`
   (совпадает с суммой строк команд по G/PTS в {tot_eq['G']:.1%}/{tot_eq['PTS']:.1%} пар), иначе — сумма по командам.
4. **Salaries:** для полной зарплаты за сезон суммировать `salary` по `(player_id, season_end)`
   (трейды разбивают выплаты на несколько строк).
5. **Покрытие join stats ↔ salaries** на пересечении 1985–2017:
   - строк stats: **{n_sub:,}**;
   - сопоставлено по `(pid, season_end)`: **{matched:,}** ({pct(matched, n_sub)});
   - нет `pid` (не найдено или неоднозначное имя): **{no_pid:,}**, из них неоднозначных **{no_pid_ambiguous:,}**;
   - `pid` есть, зарплатной строки нет: **{mapped_no_salary:,}**.
6. Неполные сезоны зарплат: **{gap_text_plain}** —
   не трактовать отсутствие зарплаты как «не играл».
7. `players.career_*` — карьерные итоги, не сезонные: для мержа на уровне сезона не использовать,
   кроме справочных атрибутов (`position`, `height`, `weight`, `birthDate`, `college`, `draft_*`).

## Риски

- **Join по имени — главный риск**: {pct(1 - norm_hits / len(norm_stats), 1)} имён не сопоставлены;
  неоднозначных нормализованных имён: **{len(ambiguous)}** ({no_pid_ambiguous:,} строк stats в 1985–2017 затронуто).
  Точный мерж без внешнего ID-маппинга невозможен.
- `Year == season_end` — соглашение подтверждено выборкой, но проверять на новых данных.
- Неполные сезоны зарплат: {gap_years_text}; 2018 есть только в зарплатах.
- Аномалии источников: {blank_ss:,} пустых строк stats, {null_teams:,} строки зарплат без команды, `"-"` в `players.career_*`.
- Денежные суммы не приведены к инфляции; межгодовые сравнения некорректны без дефлятора.
"""
    (OUT / "README.md").write_text(readme, encoding="utf-8")

    print(f"written: {', '.join(sorted(p.name for p in OUT.glob('*.md')))}")
    print(
        f"stats rows={ss_meta['rows']} blank={blank_ss} | "
        f"players={pl_meta['rows']} | salaries={sa_meta['rows']} | "
        f"join matched={matched}/{n_sub} ({pct(matched, n_sub)})"
    )


if __name__ == "__main__":
    main()
