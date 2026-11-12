# Паспорт: players.csv

| | |
|---|---|
| Файл | `players.csv` |
| Размер | 1,094,010 байт |
| Формат / кодировка | CSV / utf-8 |
| MD5 | `02682cebf85f274fef7ae62bc2b460e9` |
| Строк × колонок | 4,685 × 25 |
| Гранулярность | один игрок (биография и карьерные итоги), 1 строка = 1 `_id` |

Числа ниже посчитаны скриптом `make_passports.py` по исходному файлу.

## Ключи и покрытие

- `_id` уникален: **True**; это ID Basketball-Reference, он же `player_id` в зарплатах.
- Покрытие зарплат: **2,408 / 2,408** (100.0%) ID из зарплат есть в players;
  зарплатные строки есть у **2,408 / 4,685** (51.4%) игроков.
- Полных дублей имён: **46** строк (89 задействовано); после нормализации
  (регистр, пунктуация, пробелы) **43** неоднозначных имён — по имени таких игроков различать нельзя,
  только по `_id`.
- `career_*` — карьерные показатели смешанной семантики: `career_G`, `career_WS` — суммарные;
  `career_PTS/AST/TRB` — в среднем за игру; проценты — карьерные. Отсутствие значения: `"-"` или пустая ячейка
  (колонки с `"-"`: `career_FG%`, `career_FG3%`, `career_FT%`, `career_PER`, `career_TRB`, `career_WS`, `career_eFG%`).
- `height` — строка `футы-дюймы` (`6-10`), `weight` — строка с суффиксом (`240lb`), `draft_pick` — текст (`25th overall`).

## Словарь полей

| Поле | dtype | Nulls | Null % | Уникальных | Примеры / диапазон |
|---|---|---:|---:|---:|---|
| `index` | int64 | 0 | 0.0% | 4685 | 0 … 4684 (медиана 2342) |
| `_id` | object | 0 | 0.0% | 4685 | abdelal01; osheake01; okoboel01 |
| `birthDate` | object | 28 | 0.6% | 4284 | December 31, 1923; January 4, 1961; September 21, 1990 |
| `birthPlace` | object | 500 | 10.7% | 1632 | Chicago, Illinois; Philadelphia, Pennsylvania; Los Angeles, California |
| `career_AST` | float64 | 0 | 0.0% | 91 | 0 … 11.2 (медиана 1) |
| `career_FG%` | object | 0 | 0.0% | 441 | 33.3; 0.0; 50.0 |
| `career_FG3%` | object | 1128 | 24.1% | 352 | 0.0; -; 33.3 |
| `career_FT%` | object | 0 | 0.0% | 485 | -; 50.0; 100.0 |
| `career_G` | int64 | 0 | 0.0% | 1017 | 1 … 1611 (медиана 133) |
| `career_PER` | object | 0 | 0.0% | 373 | -; 11.9; 12.2 |
| `career_PTS` | float64 | 0 | 0.0% | 240 | 0 … 30.1 (медиана 5.2) |
| `career_TRB` | object | 0 | 0.0% | 138 | -; 1.0; 1.5 |
| `career_WS` | object | 0 | 0.0% | 774 | 0.0; -0.1; 0.1 |
| `career_eFG%` | object | 1128 | 24.1% | 370 | 50.0; 33.3; 0.0 |
| `college` | object | 313 | 6.7% | 731 | University of Kentucky; University of California, Los Angeles; University of North Carolina |
| `draft_pick` | object | 1316 | 28.1% | 195 | 5th overall; 2nd overall; 1st overall |
| `draft_round` | object | 1178 | 25.1% | 50 | 1st round; 2nd round; 3rd round |
| `draft_team` | object | 1178 | 25.1% | 65 | New York Knicks; Boston Celtics; Chicago Bulls |
| `draft_year` | object | 1379 | 29.4% | 75 | 1968; 1970; 1967 |
| `height` | object | 0 | 0.0% | 28 | 6-7; 6-8; 6-9 |
| `highSchool` | object | 660 | 14.1% | 2708 | Oak Hill Academy in Mouth of Wilson, Vir…; DeWitt Clinton in Bronx, New York; Hargrave Military Academy in Chatham, Vi… |
| `name` | object | 0 | 0.0% | 4639 | George Johnson; Charles Jones; Charles Smith |
| `position` | object | 0 | 0.0% | 43 | Shooting Guard; Center; Small Forward |
| `shoots` | object | 1 | 0.0% | 3 | Right; Left; Left Right |
| `weight` | object | 5 | 0.1% | 143 | 210lb; 190lb; 185lb |
