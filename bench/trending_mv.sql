-- Оптимизированный `GET /api/v1/trending?window=24h&limit=50` (без `language`) — задача 2.3.
-- Ровно тот SQL, что строит `build_trending_query` в services/pulse-api/app/queries.py
-- при этих параметрах, когда `language` не передан (плейсхолдеры server-side binding подставлены
-- буквальными значениями, чтобы файл был самостоятельным для clickhouse-client).
--
-- Источник — ghpulse.repo_stars_hourly_mv (задача 2.1, бэкфилл — задача 2.2), а не прямой скан
-- events, как в baseline bench/trending.sql (задача 1.9). Граница окна округлена к началу часа
-- (toStartOfHour) — неизбежное следствие часовой грануляции MV, подробности и измеренное
-- расхождение с точным baseline — в docs/PERFORMANCE.md. Вторичный ключ `repo_id ASC` в ORDER BY —
-- детерминированный tie-break для репозиториев с одинаковым числом звёзд на границе LIMIT (без
-- него верификация эквивалентности ловила ложное расхождение — см. docs/PERFORMANCE.md).

SELECT repo_id, any(repo_name) AS repo_name, sum(stars) AS stars
FROM ghpulse.repo_stars_hourly_mv
WHERE hour >= toStartOfHour(now() - INTERVAL 86400 SECOND)
GROUP BY repo_id
ORDER BY stars DESC, repo_id ASC
LIMIT 50
