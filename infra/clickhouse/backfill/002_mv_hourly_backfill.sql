-- 002_mv_hourly_backfill.sql — разовый бэкфилл трёх MV из 002_mv_hourly.sql существующими данными.
--
-- НЕ миграция и намеренно лежит вне infra/clickhouse/migrations/ (тот каталог смонтирован в
-- docker-entrypoint-initdb.d и выполняется ClickHouse только один раз, при первой инициализации
-- пустого volume — см. docker-compose.yml). Этот файл, наоборот, DML и не идемпотентен: повторный
-- запуск по тем же партициям задвоит суммы в SummingMergeTree, потому что INSERT добавляет строки,
-- а не заменяет их. Гонять руками ровно один раз на инстансе, где 002_mv_hourly.sql уже создал MV
-- поверх непустой ghpulse.events (задача 2.2, TASKS_DETAILED.md).
--
-- Перед запуском обязательно убедиться, что все три MV пустые (count() = 0) — если нет, кто-то уже
-- бэкфиллил или в MV уже попали строки через живой INSERT, повторный прогон исказит суммы.
--
-- Партиция за партицией — ограничивает память на GROUP BY одним месяцем вместо скана всей таблицы
-- разом; на инстансе, где писался этот файл, было три партиции (`SELECT DISTINCT toYYYYMM(created_at)
-- FROM ghpulse.events`): 202406 (13 158 385 строк, бэкфилл GH Archive 1.7), 202606 и 202607
-- (149 565 + 419 151 строк, живой поток). Список партиций может отличаться на другом инстансе —
-- получить актуальный тем же запросом перед прогоном, а не полагаться на числа ниже.
--
-- run: docker compose exec -T clickhouse clickhouse-client --multiquery < infra/clickhouse/backfill/002_mv_hourly_backfill.sql
-- (или по частям — см. руками патерн ниже, если партиций много и нужен контроль между шагами)

-- ── repo_stars_hourly_mv ─────────────────────────────────────────────────────────────────────
INSERT INTO ghpulse.repo_stars_hourly_mv
SELECT repo_id, any(repo_name) AS repo_name, toStartOfHour(created_at) AS hour, count() AS stars
FROM ghpulse.events
WHERE event_type = 'WatchEvent' AND toYYYYMM(created_at) = 202406
GROUP BY repo_id, hour;

INSERT INTO ghpulse.repo_stars_hourly_mv
SELECT repo_id, any(repo_name) AS repo_name, toStartOfHour(created_at) AS hour, count() AS stars
FROM ghpulse.events
WHERE event_type = 'WatchEvent' AND toYYYYMM(created_at) = 202606
GROUP BY repo_id, hour;

INSERT INTO ghpulse.repo_stars_hourly_mv
SELECT repo_id, any(repo_name) AS repo_name, toStartOfHour(created_at) AS hour, count() AS stars
FROM ghpulse.events
WHERE event_type = 'WatchEvent' AND toYYYYMM(created_at) = 202607
GROUP BY repo_id, hour;

-- ── language_daily_mv ────────────────────────────────────────────────────────────────────────
-- Обогащение языка на этом инстансе — 0% (задача 4.3 ещё не выполнялась, см. 002_mv_hourly.sql),
-- поэтому WHERE language != '' отфильтрует все строки и вставит 0 — это ожидаемый, а не ошибочный
-- результат; MV начнёт наполняться сама, как только заработает обогащение.
INSERT INTO ghpulse.language_daily_mv
SELECT toDate(created_at) AS day, language, count() AS events
FROM ghpulse.events
WHERE language != '' AND toYYYYMM(created_at) = 202406
GROUP BY day, language;

INSERT INTO ghpulse.language_daily_mv
SELECT toDate(created_at) AS day, language, count() AS events
FROM ghpulse.events
WHERE language != '' AND toYYYYMM(created_at) = 202606
GROUP BY day, language;

INSERT INTO ghpulse.language_daily_mv
SELECT toDate(created_at) AS day, language, count() AS events
FROM ghpulse.events
WHERE language != '' AND toYYYYMM(created_at) = 202607
GROUP BY day, language;

-- ── activity_hourly_mv ───────────────────────────────────────────────────────────────────────
-- Таблица без PARTITION BY (002_mv_hourly.sql — потолок 168 строк), но исходный SELECT всё равно
-- фильтруется по тем же трём месяцам ghpulse.events, чтобы GROUP BY на вставке видел один месяц
-- сырых событий за раз, а не всю таблицу целиком.
INSERT INTO ghpulse.activity_hourly_mv
SELECT toDayOfWeek(created_at) AS weekday, toHour(created_at) AS hour, count() AS events
FROM ghpulse.events
WHERE toYYYYMM(created_at) = 202406
GROUP BY weekday, hour;

INSERT INTO ghpulse.activity_hourly_mv
SELECT toDayOfWeek(created_at) AS weekday, toHour(created_at) AS hour, count() AS events
FROM ghpulse.events
WHERE toYYYYMM(created_at) = 202606
GROUP BY weekday, hour;

INSERT INTO ghpulse.activity_hourly_mv
SELECT toDayOfWeek(created_at) AS weekday, toHour(created_at) AS hour, count() AS events
FROM ghpulse.events
WHERE toYYYYMM(created_at) = 202607
GROUP BY weekday, hour;

-- ── сверка (ожидаемое расхождение — 0 по всем трём) ─────────────────────────────────────────
-- SELECT sum(stars) FROM ghpulse.repo_stars_hourly_mv;                        -- = countIf(event_type='WatchEvent') FROM events
-- SELECT sum(events) FROM ghpulse.language_daily_mv;                          -- = countIf(language != '') FROM events
-- SELECT sum(events) FROM ghpulse.activity_hourly_mv;                         -- = count() FROM events
--
-- Прогон на 14 052 941 событии (2026-07-17):
--   repo_stars_hourly_mv: sum(stars) = 569 725 = countIf(event_type='WatchEvent') — расхождение 0
--   language_daily_mv:    sum(events) = 0      = countIf(language != '') = 0     — расхождение 0
--   activity_hourly_mv:   sum(events) = 14 052 941 = count() FROM events         — расхождение 0
