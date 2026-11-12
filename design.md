# Дизайн: витрина эффективности игроков NBA (Java 17 + Spark 3.5)

Источник требований — `tz.md`, профили данных — `passports/*.md`.
Реализация — **Java**, один Maven-проект, два CLI-сценария (сборка и запрос), Spark в локальном режиме.
Документ фиксирует архитектуру (C4), потоки данных (DFD), контракты схем и места, где
обеспечиваются дедупликация, уникальность ключей и partition pruning. Цифры объёмов
посчитаны по текущим исходным файлам.

## 1. Границы и инварианты

В объёме: очистка трёх CSV, реестр имён с blacklist, два соединения, метрика, запись
Parquet с партиционированием по сезону, CLI запросов top/undervalued с проверкой каталога
сезонов и pruning. Вне объёма — инфляционная корректировка, ручное разрешение однофамильцев,
инкрементальные обновления, Hive/метастор, UI (см. `tz.md` §11).

Инварианты, которые проверяются в коде, а не предполагаются:

| # | Инвариант | Где обеспечивается |
|---|---|---|
| I1 | На вход J2 обе стороны имеют ровно одну строку на ключ | P3/P5 (DFD), assert в P6 |
| I2 | После J2 нет преобразующей дедупликации/агрегации — только метрика, фильтр и контрольный assert уникальности | P6 |
| I3 | `(player_id, season)` уникален в результате | assert в P6 до записи |
| I4 | В выходе ровно 9 колонок §6.1, без временных полей | финальный `select` + assert схемы |
| I5 | `efficiency` хранится как `double` без округления | P6, округление только в выводе запроса |
| I6 | Чтение — только с корня, `season` из каталогов | DatasetView |
| I7 | Каталог сезонов строится по ФС, не по диапазону 1985–2017 | SeasonCatalog |
| I8 | Перезапись корня полная и идемпотентная | PartitionWriter (static overwrite) |

## 2. Ключевые решения

| # | Решение | Почему | Следствие |
|---|---|---|---|
| A1 | Связь stats → players только через `name_norm`; имена с `count(distinct _id) > 1` целиком в blacklist | в stats нет ID; угадывать игрока запрещено ТЗ | 43 имени / 89 `_id` исключены; 326 строк stats в периоде потеряны и залогированы |
| A2 | Агрегация каждой стороны **до** J2; после J2 — только метрика, фильтр и контрольный assert | иначе TOT/командные строки и трейды размножат join | 16 606 → 13 617 и 13 612 → 13 237 до join |
| A3 | Оба соединения — broadcast малой стороны | стороны 4.6k / 13k строк; shuffle не нужен | `BroadcastHashJoin`, без sort-merge |
| A4 | `repartition(col("season"))` → `sortWithinPartitions` → `partitionBy("season")` | хеш-партиционирование не разрывает один сезон между задачами | ровно один файл на сезон, файл отсортирован |
| A5 | Полная статическая перезапись корня (`mode=overwrite`) | удаляет устаревшие партиции, повторный запуск идемпотентен | каталог сезонов всегда соответствует последней сборке |
| A6 | Каталог сезонов — обход ФС; запрос читает корень и фильтрует литералом сезона | pruning без хардкода диапазона и без полного сканирования | читается один файл партиции |
| A7 | Один shaded jar (Spark-зависимости включены), запуск через `run.sh` | `spark-submit` в окружении не установлен; запуск самодостаточен | jar ~300 МБ; `run.sh` добавляет Java 17 `--add-opens`; для `spark-submit` — Maven-профиль `spark-provided` |
| A8 | Разбор аргументов вручную, без внешней библиотеки | несколько флагов, стандартных средств достаточно | меньше зависимостей; строгая валидация и коды выхода |
| A9 | CSV читается с `header=true` без `inferSchema`; нужные колонки — `select` по имени + `cast` | Spark CSV с навязанной `StructType` маппит поля по позиции, а не по имени: частичная схема прочитала бы первые N колонок файла | все поля приходят строками; типы задаются явными cast, лишние колонки не участвуют |

## 3. C4

### 3.1. Уровень 1 — контекст

```mermaid
C4Context
    title C4 L1 - контекст системы
    Person(analyst, "Аналитик / проверяющий", "Запускает сборку и top-N запросы из CLI")
    System(mart, "NBA efficiency mart", "Собирает player x season витрину и отвечает на запросы по сезону")
    System_Ext(src, "Три исходных CSV", "Статистика, игроки, зарплаты; read-only")
    System_Ext(fs, "Файловая система", "Parquet-витрина data/player_season_efficiency")
    Rel(analyst, mart, "Запускает CLI", "java -jar")
    Rel(mart, src, "Читает при сборке", "CSV")
    Rel(mart, fs, "Пишет и читает витрину", "Parquet")
```

### 3.2. Уровень 2 — контейнеры

```mermaid
C4Container
    title C4 L2 - контейнеры
    Person(analyst, "Аналитик", "CLI")
    System_Ext(src, "Исходные CSV", "read-only")
    Container_Boundary(sys, "NBA efficiency mart") {
        Container(build, "BuildJob", "Java 17 + Spark 3.5", "Очистка, дедуп, J1/J2, метрика, запись Parquet")
        Container(query, "QueryJob", "Java 17 + Spark 3.5", "Каталог сезонов, проверка, SQL top-N")
        ContainerDb(ds, "Parquet-витрина", "local FS", "season=YYYY/part-*.parquet")
    }
    Rel(analyst, build, "build", "CLI args")
    Rel(analyst, query, "query --season", "CLI args")
    Rel(build, src, "Читает", "CSV")
    Rel(build, ds, "Перезаписывает корень", "Parquet")
    Rel(query, ds, "Читает корень + pruning", "Parquet")
```

### 3.3. Уровень 3 — компоненты

```mermaid
C4Component
    title C4 L3 - компоненты
    Container_Boundary(build, "BuildJob") {
        Component(argsB, "BuildArgs", "Java", "Флаги, дефолты, валидация")
        Component(readers, "Readers", "Spark CSV", "select нужных колонок")
        Component(registry, "NameRegistry", "Spark SQL", "normalize_name, mapping и blacklist")
        Component(stats, "StatsCollapse", "Spark SQL", "J1 по name_norm, TOT или sum(team)")
        Component(sal, "SalaryCollapse", "Spark SQL", "Фильтр, sum(salary)")
        Component(join, "JoinAndMetric", "Spark SQL", "J2, assert, efficiency, production")
        Component(writer, "PartitionWriter", "Spark", "repartition, sort, overwrite")
        Component(report, "BuildReport", "slf4j", "Счётчики раздела 9 ТЗ")
    }
    Container_Boundary(query, "QueryJob") {
        Component(argsQ, "QueryArgs", "Java", "Разбор, коды выхода")
        Component(catalog, "SeasonCatalog", "Hadoop FS", "season=YYYY + parquet")
        Component(view, "DatasetView", "Spark", "read root, temp view")
        Component(sql, "TopNQuery", "Spark SQL", "row_number, ORDER BY, LIMIT")
        Component(format, "OutputFormatter", "Java", "stdout/stderr")
    }
    ContainerDb_Ext(src, "Исходные CSV", "FS", "read-only")
    ContainerDb_Ext(ds, "Parquet-витрина", "local FS", "data/player_season_efficiency")

    Rel(argsB, readers, "пути и дефолты")
    Rel(readers, src, "читает", "CSV")
    Rel(readers, registry, "players")
    Rel(readers, stats, "stats")
    Rel(readers, sal, "salaries")
    Rel(registry, stats, "name_map, blacklist")
    Rel(stats, join, "уникальный ключ")
    Rel(sal, join, "уникальный ключ")
    Rel(join, writer, "готовые строки")
    Rel(writer, ds, "пишет", "Parquet")
    Rel(registry, report, "счётчики")
    Rel(stats, report, "счётчики")
    Rel(join, report, "счётчики")
    Rel(argsQ, catalog, "путь и сезон")
    Rel(catalog, view, "сезон валиден")
    Rel(view, ds, "root + discovery", "Parquet")
    Rel(view, sql, "temp view")
    Rel(sql, format, "top-N")
    Rel(argsQ, format, "ошибки и предупреждения")
```

Контракт компонентов: `Readers` отдаёт три `Dataset<Row>` с уже спроецированными колонками;
`NameRegistry` — два малых датасета (`name_map`, `blacklist`); `StatsCollapse` и
`SalaryCollapse` — по одной строке на ключ; `JoinAndMetric` — финальные 9 колонок;
`PartitionWriter` не знает ничего о происхождении данных. `BuildReport` — единственный
потребитель счётчиков, собирает их до записи (датасеты кэшируются, чтобы не пересчитывать план).

## 4. DFD

### 4.1. Уровень 1 — потоки данных, оба join и потери строк

```mermaid
flowchart TB
    subgraph SRC["Источники (read-only)"]
        direction LR
        F1["Seasons_Stats.xls<br/>CSV: статистика"]
        F2["players.csv<br/>CSV: игроки"]
        F3["salaries_1985to2018.csv<br/>CSV: зарплаты"]
    end

    subgraph BUILD["Сборка - BuildJob (Java 17 + Spark)"]
        direction TB
        P1["P1 - players<br/>select(_id, name), drop пустых, normalize_name<br/>count(DISTINCT _id) по name_norm"]
        D1[("D1 name_map<br/>name_norm -> player_id, player_name")]
        D2[("D2 blacklist<br/>name_norm -> список _id")]
        P2["P2 - stats<br/>select колонок, drop пустых, фильтр периода<br/>normalize_name"]
        J1["J1 - inner lookup по name_norm<br/>broadcast(D1)"]
        P3["P3 - collapse (player_id, season)<br/>TOT иначе sum по командам"]
        D3[("D3 stats<br/>1 строка на (player_id, season)")]
        P4["P4 - salaries<br/>select колонок, фильтр периода<br/>drop null, salary больше нуля"]
        P5["P5 - sum(salary)<br/>по (player_id, season_end)"]
        D4[("D4 salaries<br/>1 строка на (player_id, season_end)")]
        J2["J2 - inner join<br/>player_id = player_id<br/>AND season = season_end<br/>broadcast(D4)"]
        P6["P6 - efficiency = salary / (pts + trb + ast)<br/>production больше нуля<br/>assert unique(player_id, season)"]
        P7["P7 - repartition(season)<br/>sortWithinPartitions(efficiency DESC, player_id ASC)<br/>write partitionBy(season), overwrite"]
        D5[("D5 витрина Parquet<br/>каталоги season=YYYY, 1 файл на сезон")]
        R1["R1 build report<br/>счётчики раздела 9 ТЗ"]
    end

    subgraph QRY["Запрос - QueryJob"]
        direction TB
        P8["P8 - каталог: season=YYYY и минимум 1 parquet<br/>проверка --season"]
        P9["P9 - read root (partition discovery)<br/>WHERE season = YYYY AND games >= M<br/>row_number + ORDER BY + LIMIT"]
        OUT["stdout: rank, player_id, player_name,<br/>season, games, salary, pts, trb, ast, efficiency"]
    end

    F2 --> P1
    P1 --> D1
    P1 -.-> D2
    F1 --> P2
    D1 --> J1
    P2 --> J1
    J1 -->|не найдено / blacklist исключены| P3
    P3 --> D3
    F3 --> P4
    P4 --> P5
    P5 --> D4
    D3 --> J2
    D4 --> J2
    J2 -->|inner join, ключи уникальны| P6
    P6 -->|production больше нуля| P7
    P7 --> D5
    D5 --> P8
    P8 --> P9
    P9 --> OUT
    D2 -.-> R1
    P3 -.-> R1
    J2 -.-> R1
    P6 -.-> R1
```

Сплошные стрелки — основные данные; пунктир — счётчики в отчёт. На рёбрах показаны только
правила отсечения, без фактических объёмов (они — в таблице 4.3). Порядок строго по ТЗ §4:
сначала независимая очистка и агрегация каждой стороны, затем J2; J1 — это не join из §4.4,
а lookup внутри подготовки stats (по §4.2).

### 4.2. Где именно учтён дедуп

| # | Датасет | Ключ | Правило | Почему так | Эффект на текущих данных |
|---|---|---|---|---|---|
| D-1 | players | `name_norm` | `count(DISTINCT _id)`; ровно 1 → mapping, больше 1 → blacklist целиком | в stats нет ID; выбор «наиболее вероятного» запрещён | 4 596 имён в mapping; 43 имени / 89 игроков в blacklist |
| D-2 | stats | `(player_id, season)` | если у пары есть `Tm = "TOT"` — берётся только TOT; иначе сумма строк команд | TOT уже является суммой команд, сложение = двойной учёт | 2 989 командных строк отброшено anti-join по TOT-ключам; 16 606 → 13 617 |
| D-3 | salaries | `(player_id, season_end)` | `sum(salary)` | трейд разбивает выплату на несколько строк | 13 612 → 13 237; 375 строк свёрнуто |
| D-4 | после J2 | `(player_id, season)` | assert `count = 1`, иначе сборка падает | join не должен размножать строки; `distinct` после join запрещён | 12 124 пары, дублей 0 |
| D-5 | после J2 | `production > 0` | отсечение строк | защита от деления на ноль | −21 строка |

Повторяющиеся строки одного и того же `_id` в players не создают неоднозначности: D-1
считает именно `DISTINCT _id`. Blacklist применяется ко всем сезонам игрока, даже если
статистика есть только за часть сезонов.

### 4.3. Объёмы по этапам (текущие файлы)

| Этап | players | stats | salaries |
|---|---:|---:|---:|
| строк в файле | 4 685 | 24 691 | 14 163 |
| после удаления пустых строк | 4 685 | 24 624 (−67) | 14 163 |
| после фильтра периода 1985–2017 | 4 685 | 17 100 | 13 612 (−551 за 2018) |
| после имени / очистки | 4 596 имён в mapping; 43 в blacklist (89 `_id`) | 16 606 (−494: 326 blacklist + 168 не найдено) | 13 612 |
| после дедупа/агрегации (вход J2) | — | 13 617 (1 строка на ключ) | 13 237 (1 строка на ключ) |
| J2 (inner) | — | 12 124 (−1 493 без зарплаты) | 12 124 (−1 113 без статистики) |
| `production <= 0` | — | −21 | — |
| **результат** | | **12 103 строки, 33 сезона** | |

Строка `players` в таблице: 4 685 строк и 4 685 уникальных `_id` (дублей нет), но после
фильтра неоднозначных имён остаётся 4 596 разрешённых имён. Значения совпадают с ориентиром
ТЗ §10 (12 124 пары, 21 отсечённая строка, 12 103 строки).

Почему после дедупа отбрасывается мало — баланс строк stats:

```text
24 691 (файл) − 67 (пустые) − 7 524 (вне периода 1985–2017) = 17 100
17 100 − 494 (имя не найдено / blacklist)                     = 16 606
16 606 − 2 989 (командные строки, уже покрытые TOT)           = 13 617 пар
13 617 − 1 493 (нет зарплаты за сезон)                        = 12 124
12 124 − 21 (production <= 0)                                 = 12 103
```

Дедуп снимает только реальные дубли гранулярности, а не большую часть файла: 2 989
TOT-covered строк в stats (−18%) и 375 trade-строк в salaries (−2.8%). Остальные потери —
фильтр периода (−7 524), join по имени (−494) и inner join с зарплатами (−1 493, −11% пар).
Большая часть пар stats имеет зарплату (12 124 из 13 617, 89%), поэтому после свёртки объём
сокращается на проценты, а не в разы. В players дедуп почти ничего не снимает: файл — одна
строка на игрока, отсекаются только 89 `_id`, попавших в 43 неоднозначных имени.

## 5. Контракты данных

### 5.1. Входные проекции (читаются только эти колонки)

| Файл | Колонки | Тип после select/cast |
|---|---|---|
| `players.csv` | `_id`, `name` | string, string |
| `Seasons_Stats.xls` | `Player`, `Year`, `Tm`, `G`, `PTS`, `TRB`, `AST` | string, int, string, long, long, long, long |
| `salaries_1985to2018.csv` | `player_id`, `season_end`, `salary` | string, int, long |

CSV читается как `header=true` без `inferSchema`: все поля приходят строками, нужные колонки
выбираются по именам из header и кастуются. Навязывать частичную `StructType` нельзя:
Spark CSV сопоставляет схему с файлом **по позиции**, поэтому 7-колоночная схема
прочитала бы первые 7 колонок файла вместо `Player/Year/Tm/...`. Полная схема на 53
колонки не нужна: header даёт имена, cast — типы. `Year`/`season_end` кастуются в int;
меры и `salary` — в long через `double`, чтобы одинаково принимать `82`, `82.0` и большие
зарплаты (double точно представляет целые до 2^53); пустые значения остаются `null`.

### 5.2. Промежуточные датасеты

| Датасет | Колонки | Уникальность |
|---|---|---|
| `name_map` | `name_norm string`, `player_id string`, `player_name string` | 1 строка на `name_norm` |
| `blacklist` | `name_norm string`, `ids array<string>` | 1 строка на `name_norm` (только для отчёта) |
| `stats` | `player_id string`, `player_name string` (константа на игрока), `season int`, `games long`, `pts long`, `trb long`, `ast long` | 1 строка на `(player_id, season)` |
| `salaries` | `player_id string`, `season_end int`, `salary long` | 1 строка на `(player_id, season_end)` |
| `joined` | те же ключи + `player_name`, `salary`, меры | 1 строка на `(player_id, season)` |

`Tm`, `name_norm`, `Year`, `Player` живут только до свёртки stats. `player_name` — не
временное поле: это каноническое имя из `players`, оно переносится в результат.
`production` — временное выражение внутри P6, в выход не попадает.

### 5.3. Выходная схема (порядок select при записи)

| # | Колонка | Тип Spark | Источник |
|---|---|---|---|
| 1 | `season` | int | `Year`, партиционирующая колонка |
| 2 | `player_id` | string | `players._id` |
| 3 | `player_name` | string | `players.name` (переносится через stats) |
| 4 | `games` | int | `sum(G)` |
| 5 | `salary` | long | `sum(salary)` |
| 6 | `pts` | int | `sum(PTS)` |
| 7 | `trb` | int | `sum(TRB)`, null → 0 |
| 8 | `ast` | int | `sum(AST)`, null → 0 |
| 9 | `efficiency` | double | `salary / (pts + trb + ast)` |

При записи `partitionBy("season")` колонка `season` физически не дублируется в файле.
Важное следствие для проверок: после `spark.read.parquet(root)` Spark добавляет
восстановленную партиционирующую колонку **в конец** схемы, то есть логический порядок
будет `player_id … efficiency, season`. Это не нарушает §6.1 (набор колонок), но
нельзя проверять порядок колонок после чтения; запрос перечисляет колонки явно.

Суммы считаются в `long` и приводятся к `int` в финальном select: исходные значения
(максимум 4 029 очков) заведомо ниже границы `int`. Схема проверяется assert-ом перед записью.

### 5.4. Отбрасывание колонок до join

Порядок §4 ТЗ соблюдается буквально: проекция — первое действие с каждым файлом, **до**
очистки строк, агрегации и J2. Отбрасываются не только исходные колонки, но и временные
поля: к моменту J2 в stats не остаётся `Player`, `Year`, `Tm`, `name_norm`, в salaries —
`team`, `season`, `season_start`.

| Датасет | Отбрасывается при чтении | Временные поля (живут до своего этапа) | Что остаётся для следующих этапов |
|---|---|---|---|
| `players.csv` | 23 из 25: `index`, `career_*`, биография, драфт, `college`, `height`, `weight`, `highSchool`, `shoots`, `position` | `name_norm` — ключ J1 | `name_map` для J1: `name_norm`, `player_id`, `player_name` |
| `Seasons_Stats.xls` | 46 из 53: `Unnamed: 0`, `Pos`, `Age`, `GS`, `MP`, все проценты, `PER`, `WS`, `BPM`, `VORP`, `FG/FT/...` | `Player`, `Year`, `Tm`, `name_norm` | `player_id`, `player_name`, `season`, `games`, `pts`, `trb`, `ast` |
| `salaries_1985to2018.csv` | 5 из 8: `index`, `league`, `season`, `season_start`, `team` | — | `player_id`, `season_end`, `salary` |
| после J2 | — | `season_end`, `production` | 9 выходных колонок §5.3 |

`name_norm` после подготовки остаётся только в `name_map` — это ключ J1, а не поле результата.
J2 соединяет уже сокращённые стороны: ключи `(player_id, season)` и меры; никакие исходные
или технические колонки в него не попадают.

Сразу после проекции отбрасываются строки, у которых пусты ключевые поля: players —
`_id`/`name`, stats — `Player`/`Year`, salaries — `player_id`/`season_end`/`salary`
(и `salary <= 0`). Такие строки не доходят ни до J1, ни до J2 и не участвуют в отчёте о
blacklist. Исключение — `G`/`PTS`: по §4.2.4 их проверка стоит после свёртки, потому что
строка с пустым `PTS` может нести валидные `TRB`/`AST`; раннее удаление исказило бы суммы.
На текущих данных строк без `G`/`PTS` в периоде нет.

## 6. Алгоритмы сборки

### 6.1. `normalize_name` — одна реализация на обе стороны

```java
static Column normalizeName(Column c) {
    return trim(regexp_replace(
        regexp_replace(
            regexp_replace(lower(trim(c)), "\\*", " "),
            "[^a-z0-9 ]+", " "),
        " +", " "));
}
```

Порядок ровно по §4.1: trim → lower → метка `*` и все не-`[a-z0-9 ]` в пробел → схлопывание
пробелов → trim. Функция применяется к `players.name` и `Seasons_Stats.Player`; это
`Column`-выражение, без Python UDF и без сериализации. Акцентированные/нелатинские символы
по правилу ТЗ заменяются пробелом — это осознанное ограничение, а не баг (риск Р-1).

### 6.2. Реестр имён

```java
Dataset<Row> registry = players
    .filter(col("_id").isNotNull().and(length(trim(col("_id"))).gt(0))
        .and(col("name").isNotNull()).and(length(trim(col("name"))).gt(0)))
    .select(col("_id").as("player_id"), col("name").as("player_name"),
            normalizeName(col("name")).as("name_norm"))
    .groupBy("name_norm")
    .agg(countDistinct("player_id").as("id_count"),
         first("player_id").as("player_id"),
         first("player_name").as("player_name"),
         sort_array(collect_set("player_id")).as("ids"));

Dataset<Row> nameMap  = registry.filter(col("id_count").equalTo(1))
                                .select("name_norm", "player_id", "player_name");
Dataset<Row> blacklist = registry.filter(col("id_count").greaterThan(1))
                                 .select("name_norm", "ids");
```

`first(...)` безопасен: в текущих данных один `_id` = одна строка; повторяющиеся строки
одного `_id` не влияют на `countDistinct`. Blacklist содержит все 43 имени и полный список
`_id`; при отчёте к нему присоединяются счётчики строк stats по `name_norm`.

### 6.3. Stats: проекция, фильтры, J1, свёртка

1. Чтение с `header=true` без `inferSchema`; `select` 7 колонок по именам; `Year` → int,
   меры → long через `double`.
2. Удаление строк без `Player`/`Year`; полностью пустые строки (67 шт.) имеют пустой
   `Year`, поэтому отдельный проход по всем колонкам не нужен.
3. Фильтр `Year BETWEEN 1985 AND 2017`; `season = Year.cast(int)`.
4. `name_norm = normalize_name(Player)`; `inner` join с `broadcast(name_map)` по `name_norm`.
   Строки без совпадения и blacklist отсекаются здесь; blacklist-строки отдельно считаются
   для отчёта join-ом исходного stats по `name_norm` с blacklist.
5. Меры приводятся к `long` (nullable): `G`/`PTS` не фильтруются до свёртки — правило
   «удалить строки без `G`/`PTS`» применяется после неё, как в §4.2.4.
6. Свёртка до `(player_id, season)`:

```java
// Java API принимает список колонок как scala Seq — конверсия инкапсулирована в хелпере
static Dataset<Row> joinUsing(Dataset<Row> l, Dataset<Row> r, String how, String... using) {
    return l.join(r, JavaConverters.asScalaBuffer(Arrays.asList(using)).toSeq(), how);
}

Dataset<Row> tot    = named.filter(col("Tm").equalTo("TOT"));
Dataset<Row> totKey = tot.select("player_id", "season").distinct();      // ключи, не данные
Dataset<Row> team   = joinUsing(named.filter(not(col("Tm").equalTo("TOT"))),
                                broadcast(totKey), "left_anti", "player_id", "season");
Dataset<Row> collapsed = tot.unionByName(team)
    .groupBy("player_id", "season")
    .agg(first("player_name").as("player_name"),
         sum("G").as("games"), sum("PTS").as("pts"),
         sum("TRB").as("trb"), sum("AST").as("ast"));

// null TRB/AST считаются до замены и попадают в отчёт
Dataset<Row> stats = collapsed
    .filter(col("games").isNotNull().and(col("pts").isNotNull()))
    .withColumn("trb", coalesce(col("trb"), lit(0)))
    .withColumn("ast", coalesce(col("ast"), lit(0)));
```

7. `TRB`/`AST` null → 0 (число замен логируется); строки без `games`/`pts` удаляются.

`distinct()` здесь — только построение множества TOT-ключей до свёртки; данные не
дедуплицируются повторно. Anti-join гарантирует, что TOT-строка и строки команд не
складываются. `player_name` — каноническое имя из `name_map`, константа на `player_id`;
оно нужно выходной схеме, поэтому переносится через свёртку (исходный `Player` при этом
отбрасывается, как требует §4.2.4). После свёртки остаются ключи, имя и меры.

### 6.4. Salaries

1. Чтение с `header=true` без `inferSchema`; `select` трёх колонок по именам;
   `season_end` → int, `salary` → long через `double`.
2. Фильтр `season_end BETWEEN 1985 AND 2017`, непустые ключи (null и пустая строка),
   `salary > 0`.
3. `groupBy(player_id, season_end).agg(sum(salary))` — ровно одна строка на ключ.
4. `salary` остаётся `long`, пропуски не заполняются и не оцениваются.

### 6.5. J2 и проверки до метрики

```java
Dataset<Row> joined = stats.as("s")
    .join(broadcast(salaries.as("l")),
          col("s.player_id").equalTo(col("l.player_id"))
              .and(col("s.season").equalTo(col("l.season_end"))),
          "inner")
    .select(col("s.player_id"), col("s.season"), col("s.player_name"),
            col("s.games"), col("l.salary"), col("s.pts"), col("s.trb"), col("s.ast"));
```

Отдельного фильтра «без зарплаты» после J1 нет: J2 — `inner join`, и именно он отсекает
такие пары (в отчёте — `no_salary`). Фильтровать игрока целиком после J1 нельзя: зарплата
задана на `(player_id, season)`, и игрок может иметь контракт в других сезонах. Ранний
semi-join по зарплатным ключам дал бы тот же результат, но это второй проход по тем же
данным и нарушение порядка «сначала агрегация stats, затем join с salaries».

Стоимостная проверка: J2 — broadcast hash join. Чтобы отсеять пару semi-join-ом, её ключ
всё равно проверяется по тем же salaries, после чего J2 проверяет выжившие повторно — два
lookup вместо одного. Фильтр на уровне игрока тем более не помогает: из 1 493 пар без
зарплаты только 308 (239 игроков) относятся к игрокам без зарплаты в периоде; остальные
1 185 — сезонные пропуски у игроков с контрактами в других сезонах. Early semi-join/Bloom
имел бы смысл при shuffle/sort-merge join и больших объёмах, но не при 13k строк и
broadcast.

Перед метрикой — assert уникальности одним действием (`joined` кэшируется, чтобы assert,
отчёт и метрика не пересчитывали план):

```java
long dup = joined.groupBy("player_id", "season").count()
                 .agg(max("count").as("m")).head().getLong(0);
if (dup > 1) throw new IllegalStateException("duplicate (player_id, season) after join");
```

Нарушение — ошибка сборки, а не повод для `distinct`. Это единственный `groupBy` после J2:
контрольный запрос, который не участвует в формировании данных и не меняет их. Null-ключи
после join невозможны: обе стороны уже отфильтрованы; дополнительный фильтр не нужен
(но `player_id`/`season` входят в assert через `count`).

### 6.6. Метрика, фильтр, схема

```java
Dataset<Row> result = joined
    .withColumn("production", col("pts").plus(col("trb")).plus(col("ast")))
    .filter(col("production").gt(0))
    .withColumn("efficiency",
        col("salary").cast("double").divide(col("production").cast("double")))
    .select(col("season"), col("player_id"), col("player_name"), col("games"),
            col("salary"), col("pts"), col("trb"), col("ast"), col("efficiency"));
```

`production` не попадает в результат. Перед записью проверяется набор и типы 9 колонок
(assert по `result.schema()`); при расхождении — исключение с фактической схемой.

### 6.7. Запись и физический порядок

```java
result.repartition(col("season"))
      .sortWithinPartitions(col("efficiency").desc(), col("player_id").asc())
      .write().mode(SaveMode.Overwrite).partitionBy("season")
      .parquet(output);
```

- `repartition(col("season"))` — хеш по сезону: строки одного сезона не разрываются между
  задачами. Каждый сезон пишется из одной задачи, поэтому в `season=YYYY/` появляется ровно
  один Parquet-файл (служебные `_SUCCESS`, `_committed_*`, `_started_*` — не данные).
- `sortWithinPartitions` сортирует строки задачи целиком; сортировка одного сезона
  гарантирована, так как все его строки в одной задаче.
- `mode(overwrite)` в статическом режиме (по умолчанию `partitionOverwriteMode=static`)
  удаляет содержимое корня до записи: устаревшие партиции не остаются, повторная сборка
  идемпотентна. Каталог `--output` считается принадлежащим джобе; посторонние файлы в нём
  будут удалены.
- AQE может укрупнить партиции, но не разрывает одинаковые ключи между задачами; на
  «один файл на сезон» это не влияет.

### 6.8. Отчёт сборки

Счётчики собираются по кэшированным датасетам до записи, выводятся в stdout/slf4j.
Формат строк — стабильные префиксы, чтобы отчёт можно было проверить скриптом:

```text
[players]  rows=4685 (unique _id=4685); names=4639; mapping=4596; blacklist=43 names / 89 ids
[stats]    rows=24691 -> 24624 (empty) -> 17100 (period) -> 16606 (named) -> 13617 keys
[named]    matched_rows=16606; not_found=168 rows / 28 names; blacklist=326 rows / 30 names
[blacklist] name_norm=...; ids=[...]; stats_rows=N        # по строке на имя
[salaries] rows=14163 -> 13612 (period) -> 13237 keys
[join]     stats_keys=13617; salary_keys=13237; joined=12124; no_salary=1493; no_stats=1113; dupes=0
[cleanup]  null_trb_replaced=0; null_ast_replaced=0; dropped_no_g_or_pts=0
[metric]   production<=0 dropped=21
[result]   rows=12103; seasons=33
```

Обязательные по §9 поля: строки до/после очистки и после агрегации по каждому датасету;
число уникальных ключей на входе join; сопоставлено/не найдено/blacklist (с `name_norm`,
`_id` и числом исключённых строк); исключено из-за отсутствия зарплаты; пары зарплат без
статистики; TRB/AST null → 0; `production <= 0`; строки и сезоны результата.

## 7. Запрос

### 7.1. CLI и коды выхода

| Сценарий | Аргументы | Дефолты |
|---|---|---|
| build | `--stats`, `--players`, `--salaries`, `--output` | файлы и `data/player_season_efficiency` от cwd (как в §8.1 ТЗ) |
| query | `--season` (обязателен), `--type top\|undervalued`, `--limit`, `--min-games`, `--data`, `--explain` | тип `top`; limit 5 для top и 10 для undervalued; `min-games` 20; data от корня проекта |

Разбор аргументов — вручную; `--season` парсится `Integer.parseInt` (пустое/нецелое —
ошибка), `--limit > 0`, `--min-games >= 0`, `--type` из двух значений. Относительные
явные пути разрешаются от cwd; дефолты build — от cwd, дефолты query — от корня проекта
(§7.1.5). Корень проекта определяется по расположению jar/classes (снимаются хвостовые
компоненты `target` и `classes`); если в нём нет `players.csv` — fallback на cwd.
Поэтому дефолтный `--data` не зависит от cwd.

| Код | Когда |
|---:|---|
| 0 | успех, включая «нет кандидатов после min_games» |
| 2 | ошибка аргументов (сезон/limit/min-games/type) |
| 3 | корень отсутствует или каталог сезонов пуст |
| 4 | сезона нет в каталоге |
| 5 | ошибка Spark/ФС при чтении или запросе |

### 7.2. Каталог сезонов

`SeasonCatalog.scan(root)` через Hadoop `FileSystem`:

1. Корень не существует → код 3, `датасет не найден: <path>`.
2. Для каждого элемента корня: каталог, имя строго `season=(\d{4})` (ровно 4 цифры),
   внутри есть хотя бы один файл `*.parquet` (без учёта `_SUCCESS`/`_committed_*`).
   Прочие каталоги и файлы игнорируются. Это и есть «каталог сезонов»; диапазон
   1985–2017 не участвует.
3. Список пуст → код 3, `датасет пуст: <path>`.
4. `--season` не найден → код 4 и точное сообщение:
   `сезон YYYY отсутствует в каталоге; доступные сезоны: 1985, 1986, ...` (по возрастанию).
5. Найден → читаем корень; фильтр `games >= min_games` может дать 0 строк — это успех (0),
   заголовок + предупреждение в stderr.

Проверка каталога не запускает Spark-job: это ~33 операции `listStatus`, что дешевле
валидации через чтение датасета. Гонка «каталог удалён между проверкой и чтением» —
ошибка Spark, код 5.

### 7.3. Чтение и pruning

```java
Dataset<Row> ds = spark.read().parquet(root);        // только корень, без basePath
ds.createOrReplaceTempView("player_season_efficiency");
```

`season` восстанавливается из имён каталогов (тип int). Ручное перечисление сезонов,
чтение `season=YYYY` напрямую и схема с объявленным `season` запрещены. Фильтр сезона —
целочисленный литерал в SQL (значение уже провалидировано), поэтому Catalyst формирует
`PartitionFilters: ... (season = YYYY)`. `--explain` печатает план в stderr (stdout остаётся
данными); без флага план доступен в DEBUG-логе. Проверка: в плане `FileScan parquet` один
файл партиции.

### 7.4. SQL top-N

```sql
SELECT ROW_NUMBER() OVER (ORDER BY efficiency {DESC|ASC}, player_id ASC) AS rank,
       player_id, player_name, season, games, salary, pts, trb, ast, efficiency
FROM player_season_efficiency
WHERE season = {season} AND games >= {min_games}
ORDER BY efficiency {DESC|ASC}, player_id ASC
LIMIT {limit}
```

`DESC` для `--type top`, `ASC` для `undervalued`. Тай-брейк `player_id ASC` делает порядок
детерминированным. Окно считается по уже отфильтрованному набору (≤ одной партиции),
`LIMIT` ограничивает вывод. В SQL подставляются только провалидированные `int`-литералы и
значение enum, поэтому интерполяция безопасна и не мешает pruning. Никакой запрос не
полагается на физический порядок файла.

### 7.5. Вывод

- stdout: строка заголовка и строки результата, колонки через табуляцию:
  `rank	player_id	player_name	season	games	salary	pts	trb	ast	efficiency`.
- `salary` — целое, `efficiency` — `%.6f` (округление только на выводе; в Parquet значение
  не округляется). Форматирование — через `Locale.ROOT`, чтобы в русской локали не
  появилась десятичная запятая.
- stderr: предупреждения `найдено M < N кандидатов` или `нет кандидатов` (при 0 строк
  после `min_games` заголовок всё равно печатается), ошибки с кодами.
- Spark-логи приглушаются до WARN, чтобы stdout оставался машиночитаемым.

## 8. Почему путь данных оптимален

1. **Проекция до очистки и join.** Из 53/25/8 колонок выбираются 7/2/3 ещё при чтении;
   остальные не участвуют в преобразованиях и cast. `Tm`/`name_norm`/`Year` уходят при
   свёртке stats, `team`/`season_start` — при очистке salaries, поэтому J2 видит только
   ключи и меры. Чтение без `inferSchema` — один проход, без отдельного прохода на
   определение типов.
2. **Ранние фильтры.** Строки без ключей (players — `_id`/`name`, stats — `Player`/`Year`,
   salaries — `player_id`/`season_end`), пустые строки stats, `Year` вне 1985–2017 и
   `season_end=2018` отсекаются до нормализации и join; в salaries также `null`/пустой
   `salary` и `salary <= 0`.
3. **Агрегация до join.** 16 606 → 13 617 и 13 612 → 13 237; join получает по одной строке
   на ключ, поэтому не размножает строки и не требует последующего `distinct`.
4. **Broadcast-соединения.** `name_map` (4 596 строк) и salaries (13 237 строк)
   транслируются в stats: `BroadcastHashJoin` без shuffle и sort-merge. Оба датасета
   заведомо малы; hint `broadcast()` фиксирует выбор независимо от порога.
5. **Один узкий этап после J2.** Метрика и фильтр — map-операции без обмена; единственный
   shuffle сборки — `repartition(season)` перед записью.
6. **Сортировка только внутри партиции.** `sortWithinPartitions` не требует глобального
   sort-shuffle; при этом каждый файл отсортирован по `efficiency DESC, player_id ASC`.
7. **Запись без лишних файлов.** Хеш-партиционирование по сезону + `partitionBy` даёт ровно
   33 файла (по одному на сезон) и полную перезапись корня.
8. **Запрос без полного сканирования.** Каталог — обход ФС, затем pruning по `season`:
   читается один файл (~370 строк на сезон); `ORDER BY` сортирует только эту партицию,
   `LIMIT` ограничивает вывод. Валидация сезона не требует Spark-job.
9. **Один проход по каждому источнику.** Каждый файл читается один раз; промежуточные
   датасеты кэшируются для отчёта и переиспользуются в join, повторных чтений нет.

## 9. Риски и ограничения

| # | Риск | Митигация / следствие |
|---|---|---|
| Р-1 | Нормализация по ТЗ превращает акценты и нелатинские символы в пробелы: возможны коллизии имён и потери | на текущих данных не найдено 28 имён / 168 строк; blacklist 43 имени / 89 `_id`, из них 30 имён / 326 строк со статистикой в периоде; всё считается в отчёте |
| Р-2 | Сезоны 1987 и 1990 неполны в зарплатах | пропуски не заполняются; отсутствие зарплаты ≠ отсутствие контракта |
| Р-3 | Зарплаты не приведены к инфляции | метрика сравнивается только внутри сезона; межсезонные сравнения запрещены |
| Р-4 | `TRB`/`AST` null трактуются как 0 | число замен в отчёте; на текущих данных 0 |
| Р-5 | Физическая сортировка файла не является контрактом запроса | каждый запрос содержит `ORDER BY`; top-5 может использовать early-stop, но не обязан |
| Р-6 | Полная перезапись корня удаляет всё в `--output` | путь считается принадлежащим сборке; это требование §6.2 |
| Р-7 | `season` после чтения оказывается последней колонкой | запросы перечисляют колонки явно; проверяется набор, а не порядок |
| Р-8 | Добавленный вручную каталог `season=YYYY` меняет каталог сезонов | это требуемое поведение §6.2.5; некорректный шаблон игнорируется |
| Р-9 | Один сезон может оказаться в задаче вместе с другими | на «1 файл на сезон» не влияет: каждая задача пишет отдельный файл в каталог каждого сезона |
| Р-10 | Spark 3.5 на Java 17 требует `--add-opens` при прямом `java -jar` | `run.sh` содержит набор опций; для `spark-submit` — профиль `spark-provided` |

## 10. Проверка после реализации

```bash
mvn -q package
./run.sh build
./run.sh query --season 1991 --type top --limit 5 --explain
./run.sh query --season 1991 --type undervalued --limit 10
find data/player_season_efficiency -name '*.parquet' | wc -l   # 33
```

Соответствие критериям §10 ТЗ:

| # | Критерий | Чем обеспечен |
|---:|---|---|
| 1 | 33 партиции, по одному файлу | repartition(season) + partitionBy; проверка `find` |
| 2 | идемпотентность | статическая перезапись корня; повторный запуск и сравнение отчёта |
| 3 | `season` без схемы при чтении корня | partition discovery, `spark.read.parquet(root)` |
| 4 | ровно 9 колонок | финальный `select`, assert схемы до записи |
| 5 | дедуп до join, нет пост-join преобразований | D-2/D-3; после J2 только метрика/фильтр, плюс контрольный assert D-4 (не меняет данные) |
| 6 | `efficiency = salary / production` (±1e-9) | P6; сверка SQL-выражением |
| 7 | N строк, порядок, сезон, тай-брейк | SQL `ORDER BY … , player_id`, `LIMIT` |
| 8 | `PartitionFilters` и один файл | литерал сезона, `--explain`, физический план |
| 9 | нет сезона ≠ нет кандидатов | каталог + коды 4/0 и разные сообщения |
| 10 | абсолютный `--data`, независимость от cwd | resolved path + чтение по абсолютному пути |
| 11 | физический порядок в файле | `sortWithinPartitions`; последовательное чтение одной партиции |
| 12 | отчёт §9 | BuildReport, поля перечислены в 6.8 |

## 11. Структура проекта (предлагаемая)

```text
pom.xml                                  # Java 17, Spark 3.5.x, maven-shade, профиль spark-provided
run.sh                                   # java --add-opens ... -jar target/nba-efficiency.jar "$@"
src/main/java/ru/ppp/lab4/
  Main.java                              # build|query, коды выхода
  common/Args.java                       # ручной разбор, валидация
  common/Names.java                      # normalizeName(Column)
  common/SeasonCatalog.java              # обход ФС, season=YYYY
  common/ProjectPaths.java               # корень проекта, дефолты
  common/Spark.java                      # SparkSession (local[*])
  build/BuildJob.java                    # оркестрация P1-P7
  build/Readers.java                     # 3 CSV: header=true, select по имени, cast
  build/NameRegistry.java                # D1/D2
  build/StatsCollapse.java               # J1, TOT/team
  build/SalaryCollapse.java              # очистка и sum
  build/JoinAndMetric.java               # J2, assert, efficiency
  build/PartitionWriter.java             # repartition/sort/overwrite
  build/BuildReport.java                 # счётчики §9
  query/QueryJob.java                    # каталог, view, SQL, вывод
```

Компоненты C4 соответствуют пакетам/классам один-к-одному; мелкие утилиты (`Names`,
`ProjectPaths`) не выделяются в отдельные C4-компоненты. Shade-плагин обязан
подключить `ServicesResourceTransformer` и исключить подписи `META-INF/*.SF|DSA|RSA`,
иначе jar падает на старте.
