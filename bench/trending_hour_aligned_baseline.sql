-- Вспомогательный baseline для сверки эквивалентности `bench/trending_mv.sql` (задача 2.3) — НЕ
-- то, что реально исполняет `build_trending_query` (тот путь без `language` теперь всегда идёт
-- через `repo_stars_hourly_mv`, см. app/queries.py). Существует только чтобы изолировать одну
-- переменную: тот же прямой скан events, что и в bench/trending.sql (задача 1.9), но с той же
-- границей окна (toStartOfHour) и тем же tie-break (repo_id ASC), что и trending_mv.sql — иначе
-- сравнение с исходным bench/trending.sql ловит два расхождения сразу (точность окна и
-- недетерминированный ORDER BY) и не отвечает на вопрос "корректно ли MV агрегирует", который
-- этот файл и призван проверить.
--
-- Результат должен побайтово совпасть с bench/trending_mv.sql — это и есть доказательство
-- того, что repo_stars_hourly_mv не потеряла и не задвоила ни одного WatchEvent при бэкфилле
-- (задача 2.2) и корректно агрегирует новые вставки.

SELECT repo_id, any(repo_name) AS repo_name, count() AS stars
FROM ghpulse.events
WHERE event_type = 'WatchEvent'
  AND created_at >= toStartOfHour(now() - INTERVAL 86400 SECOND)
GROUP BY repo_id
ORDER BY stars DESC, repo_id ASC
LIMIT 50
