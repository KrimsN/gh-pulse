-- Бенчмарк baseline `/api/v1/trending?window=24h&limit=50` (без `language`).
-- Ровно тот SQL, что строит `build_trending_query` в services/pulse-api/app/queries.py
-- при этих параметрах (плейсхолдеры server-side binding подставлены буквальными значениями,
-- чтобы файл был самостоятельным для clickhouse-client).
--
-- Задача 1.9 — честный baseline на прямом скане `events`, до materialized view (задача 2.1/2.3).

SELECT repo_id, any(repo_name) AS repo_name, count() AS stars
FROM ghpulse.events
WHERE event_type = 'WatchEvent'
  AND created_at >= now() - INTERVAL 86400 SECOND
GROUP BY repo_id
ORDER BY stars DESC
LIMIT 50
