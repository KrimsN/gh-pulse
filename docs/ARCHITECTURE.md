# Архитектура

## Идея

Сервис в реальном времени поглощает публичный поток событий GitHub (пуши, PR, звёзды, форки,
issues) и отдаёт по ним аналитику через API за десятки миллисекунд, накапливая сотни миллионов
событий в ClickHouse.

**Источники данных:**
- [GH Archive](https://www.gharchive.org/) — почасовые дампы всех публичных событий GitHub с 2011
  года (`https://data.gharchive.org/YYYY-MM-DD-H.json.gz`, ~1–3 млн событий в час). Основной
  источник для бэкфилла.
- [GitHub Events API](https://api.github.com/events) — живой поток (~5000 запросов/час с токеном).
  Контур реального времени.

## Схема

```mermaid
flowchart LR
    subgraph Источники
        A1[GitHub Events API<br/>live poll]
        A2[GH Archive<br/>исторические дампы]
    end

    subgraph "gh-collector (Go)"
        B[воркер-пул, дедуп, батчинг]
    end

    subgraph Шина
        K[(Redpanda / Kafka<br/>topic: gh.events<br/>6 партиций, ключ event_id)]
    end

    subgraph "pulse-consumer (Python)"
        C[aiokafka → батч-инсерт]
    end

    subgraph Хранение
        CH[(ClickHouse<br/>события + materialized views)]
        PG[(PostgreSQL<br/>API-ключи,<br/>сохранённые отчёты)]
        R[(Redis<br/>кэш агрегатов, rate limit)]
    end

    subgraph "pulse-api"
        F[FastAPI<br/>async, Pydantic, OpenAPI]
    end

    subgraph Наблюдаемость
        O[OpenTelemetry → Prometheus → Grafana]
    end

    A1 --> B
    A2 --> B
    B --> K
    K --> C
    C --> CH
    F --> CH
    F --> PG
    F --> R
    B -.-> O
    C -.-> O
    F -.-> O
```

Ключевой шов архитектуры — разделение OLTP (PostgreSQL: метаданные, ключи, сохранённые отчёты) и
OLAP (ClickHouse: события и агрегаты). Обоснование — [ADR 0001](adr/0001-clickhouse-for-olap-postgres-for-oltp.md).

## Стек

| Слой | Технология | Обоснование |
|---|---|---|
| Ingest | **Go** | Высокочастотный I/O-bound поток fetch/produce — см. [ADR 0002](adr/0002-go-for-ingest-python-for-api.md) |
| Шина событий | **Redpanda** (Kafka-совместимая) | См. [ADR 0003](adr/0003-redpanda-as-kafka-broker.md) |
| API | **FastAPI** | Async, Pydantic-валидация, автогенерация OpenAPI |
| OLAP | **ClickHouse** | См. [ADR 0001](adr/0001-clickhouse-for-olap-postgres-for-oltp.md) |
| OLTP | **PostgreSQL** | См. [ADR 0001](adr/0001-clickhouse-for-olap-postgres-for-oltp.md) |
| Кэш | **Redis** | Кэш горячих агрегатов и rate limiting по API-ключу |
| Драйвер PostgreSQL | **asyncpg** + сырой SQL | Аналитический слой уже работает через сырой SQL к ClickHouse; ORM для PostgreSQL добавил бы второй стиль работы с БД без выигрыша на этом масштабе |
| Миграции | **Alembic** (PostgreSQL) + версионированные `.sql` (ClickHouse) | Раздельные механизмы под каждый датастор — общего инструмента, одинаково удобного для OLTP и OLAP, нет |
| Тесты | **pytest + testcontainers** | Реальные ClickHouse/PostgreSQL/Kafka в тестах — моки датастора маскируют расхождения с реальным диалектом и поведением при сбое |
| Линт/типы | **ruff + mypy --strict** | Быстрая статическая проверка |
| Пакеты | **uv** | Единый резолвер зависимостей и виртуальное окружение |
| Наблюдаемость | **OpenTelemetry → Prometheus → Grafana** | Трассировка, метрики и дашборды на одном открытом стеке |
| Нагрузочное тестирование | **k6** | Сценарии как код, встраивается в CI |
| CI | **GitHub Actions** | Линт, типы, тесты, сборка образов на каждый пуш |
| Деплой | **docker-compose → Helm-чарт** | Compose для «клонируй и запусти» локально, Helm — для деплоя в Kubernetes |

## Порты и переменные окружения

**Порты и хосты (docker-compose, сеть `ghpulse`):**

| Сервис | Хост в compose | Порт | Наружу |
|---|---|---|---|
| ClickHouse | `clickhouse` | 8123 (HTTP), 9000 (native) | 8123, 9000 |
| PostgreSQL | `postgres` | 5432 | 5432 |
| Redpanda (Kafka API) | `redpanda` | 9092 | 9092 |
| Redis | `redis` | 6379 | 6379 |
| Prometheus | `prometheus` | 9090 | 9090 |
| Grafana | `grafana` | 3000 | 3000 |
| pulse-api | `pulse-api` | 8000 | 8000 |
| pulse-consumer | `pulse-consumer` | 8001 (metrics) | 8001 |

**Переменные окружения** (единые имена во всех сервисах, значения — в локальном `.env`, который в
git не коммитится):

```dotenv
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=ghpulse
POSTGRES_DSN=postgresql://ghpulse:ghpulse@postgres:5432/ghpulse
KAFKA_BROKERS=redpanda:9092
KAFKA_TOPIC=gh.events
KAFKA_DLQ_TOPIC=gh.events.dlq
KAFKA_TOPIC_PARTITIONS=6           # потолок параллельности консьюмера; уменьшить потом нельзя
KAFKA_TOPIC_RETENTION_MS=86400000  # 24ч — максимально допустимый простой консьюмера
REDIS_URL=redis://redis:6379/0
GITHUB_TOKEN=            # для live-поллинга Events API; для GH Archive не нужен
LOG_LEVEL=INFO
```

### Топики шины событий

Создаются автоматически при `docker compose up` одноразовым контейнером `redpanda-init`
(`infra/redpanda/create-topics.sh`) — ручных шагов не требуется. Обоснование каждой строки таблицы,
включая измеренный перекос по кандидатам на ключ, — [ADR 0008](adr/0008-gh-events-topic-design.md).

| | `gh.events` | `gh.events.dlq` |
|---|---|---|
| Назначение | основной поток событий | битые сообщения (dead-letter) |
| Ключ сообщения | `event_id` | `event_id` |
| Партиции | 6 | 1 |
| Репликация | 1 (одна нода в dev-стеке) | 1 |
| `cleanup.policy` | `delete` | `delete` |
| `retention.ms` | 86 400 000 (24 ч) | 604 800 000 (7 суток) |
| `retention.bytes` | не задан | не задан |
| `compression.type` | `zstd` | `zstd` |
| `message.timestamp.type` | `LogAppendTime` | `LogAppendTime` |

Ключ `event_id` даёт равномерность по построению: значения уникальны, поэтому раскладка не зависит
от того, как меняется состав потока. Измеренный перекос — 1.005 и 1.012 на часах из разных эпох
данных против 1.717 у `actor_id`, где один бот даёт 13% событий часа.

Retention ограничен временем и намеренно не ограничен размером: лимит по размеру — единственный
механизм, способный удалить непрочитанное при живом консьюмере, то есть молча нарушить at-least-once
([ADR 0004](adr/0004-at-least-once-delivery-idempotent-inserts.md)). Отсюда правило: **максимально
допустимый простой консьюмера равен `retention.ms`**.

## Модель данных

### Каноническая схема события (внутренний контракт)

Go-коллектор нормализует и GH Archive, и Events API к этому виду; консьюмер вставляет ровно эти поля
в ClickHouse. Источник истины — `services/gh-collector/internal/model/event.go`, Python-сторона
дублирует как Pydantic-модель. Схема переживает обе эпохи данных ([ADR 0007](adr/0007-hybrid-data-epochs.md)):
все поля берутся с верхнего уровня события, где формат не менялся.

Ниже — событие **после** нормализации, ровно в том виде, в каком оно едет в Kafka: `.id` уже
приведён к числу, отсутствующий `.org` — к нулю. Сырая форма от GitHub отличается, и разбирает её
только коллектор.

```jsonc
{
  "event_id":    48572934012,            // .id — в источнике строка, здесь и в CH UInt64
  "event_type":  "WatchEvent",           // .type
  "created_at":  "2026-06-01T15:00:03Z",
  "actor_id":    1234567,                // .actor.id (число уже в источнике)
  "actor_login": "octocat",              // .actor.login
  "repo_id":     9876543,                // .repo.id
  "repo_name":   "octocat/Hello-World",  // .repo.name — всегда в форме owner/name
  "org_id":      0,                      // .org.id; 0 = вне организации — org есть у 14% событий
  "language":    "",                     // в событии отсутствует; заполняется обогащением (4.3)
  "payload_size": 512,                   // длина .payload в байтах; несопоставима между эпохами (ADR 0007)
  "ref":         "refs/heads/main"       // .payload.ref — только Push/Create/DeleteEvent, иначе ""
}
```

**Особенности данных GitHub** (проверено на реальных часах архива, см. [ADR 0007](adr/0007-hybrid-data-epochs.md)):
- `WatchEvent` — это звезда (star). GitHub так и не переименовал этот тип события;
  `payload.action` всегда `"started"` (194 из 194 в проверенном часе). Основа эндпоинта trending.
- **Форма `PushEvent.payload` зависит от эпохи.** До октября 2025:
  `{push_id, size, distinct_size, ref, head, before, repository_id, commits:[...]}`. После —
  `{push_id, ref, head, before, repository_id}`: `commits`, `size` и `distinct_size` GitHub больше
  не отдаёт. Коллектор нормализует обе формы; поля, которых нет в новой, не обязательны.
- `PullRequestEvent.payload`: `{action, number, pull_request:{...}}`, у части событий добавляются
  `label`/`labels`/`assignee`/`assignees` — набор ключей не фиксирован.
- `ForkEvent.payload`: `{action, forkee:{...}}`. `IssuesEvent.payload`: `{action, issue:{...}}`
  (плюс те же необязательные `label`/`assignee`).
- **`ref` есть в payload у `PushEvent`, `CreateEvent` и `DeleteEvent`, но его форма зависит от типа
  события.** `PushEvent` отдаёт полностью квалифицированный ref (`refs/heads/main` — 126 758 из
  126 758 в проверенном часе), а `CreateEvent` и `DeleteEvent` — голое короткое имя (`main`,
  `gh-pages`, `v1.0.0` — 14 998 из 14 998 и 6 159 из 6 159 соответственно; у `DeleteEvent` это в
  6% случаев тег, а не ветка, что видно только по `payload.ref_type`). Колонка `ref` хранит значение
  как пришло, без приведения к общей форме: нормализация означала бы дописывать `refs/heads/`
  наугад, тогда как `ref_type` у `CreateEvent`/`DeleteEvent` тега и ветки различает, а у `PushEvent`
  его нет вовсе. Следствие для запросов: одна и та же ветка выглядит как `refs/heads/main` в push и
  как `main` в create — группировать по `ref` поперёк типов событий нельзя.
- `language` в событии отсутствует — ни на верхнем уровне, ни в payload. Даже там, где поле есть
  структурно (`ForkEvent.payload.forkee.language`), GitHub отдаёт `null`. Заполняется отдельным
  обогащением через GitHub API; до обогащения `language=''`, и эндпоинты с фильтром по языку
  работают по обогащённому подмножеству.
- `org` присутствует не всегда — в проверенном часе у 20 842 событий из 149 565 (13.9%). Кодируется
  нулём, а не `Nullable(UInt64)`: организации с `id = 0` в GitHub не существует, поэтому ноль
  однозначно читается как «событие вне организации», и null-маска на весь корпус ради этого
  признака не нужна.
- Файлы Archive — по часам UTC: `https://data.gharchive.org/YYYY-MM-DD-H.json.gz` (H = 0..23, без
  ведущего нуля). **Объём часа зависит от эпохи:** ~150 тыс. событий (20 МБ gz) в 2026 против
  ~257 тыс. (140 МБ gz) в 2024 — подробности и причина в [ADR 0007](adr/0007-hybrid-data-epochs.md).

### ClickHouse — сырые события

Форма колонок выведена из реального часа архива, а не из спецификации API; обоснование каждого
выбора — в комментариях миграции [`001_events.sql`](../infra/clickhouse/migrations/001_events.sql).

```sql
CREATE TABLE ghpulse.events (
    event_id     UInt64,                   -- максимум 1.27e10 — в UInt32 не помещается
    event_type   LowCardinality(String),   -- 15 уникальных значений на весь поток
    created_at   DateTime CODEC(Delta, ZSTD),
    actor_id     UInt64,
    actor_login  String,                   -- 66 тыс. уникальных за час: для LowCardinality слишком много
    repo_id      UInt64,
    repo_name    String,                   -- 77 тыс. уникальных за час
    org_id       UInt64 DEFAULT 0,         -- 0 = вне организации; org есть лишь у 14% событий
    language     LowCardinality(String),   -- обогащение
    payload_size UInt32,
    ref          String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (event_type, created_at, repo_id)
SETTINGS index_granularity = 8192;
```

**TTL у таблицы намеренно нет.** Корпус — исторический архив, и его ценность в глубине: политика
«удалять старше N лет» уничтожила бы бэкфилл, ради которого он грузится ([ADR 0007](adr/0007-hybrid-data-epochs.md)).
Причём не постепенно: ClickHouse отбрасывает просроченные строки уже на вставке, поэтому загрузка
старых архивов молча завершалась бы нулевым результатом. Чистка при необходимости — явная,
через `DROP PARTITION`.

### ClickHouse — materialized view для предагрегации

```sql
CREATE MATERIALIZED VIEW repo_stars_hourly_mv
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (repo_id, hour)
AS SELECT
    repo_id,
    any(repo_name) AS repo_name,
    toStartOfHour(created_at) AS hour,
    count() AS stars
FROM ghpulse.events
WHERE event_type = 'WatchEvent'
GROUP BY repo_id, hour;
```

Запрос «топ репозиториев по звёздам за час» превращается из полного скана событий в чтение
агрегата. Измеренный эффект — в [docs/PERFORMANCE.md](PERFORMANCE.md) после первого прогона
бенчмарка.

### PostgreSQL — метаданные

```sql
CREATE TABLE api_keys (
    id          BIGSERIAL PRIMARY KEY,
    key_hash    TEXT NOT NULL UNIQUE,      -- SHA-256 от ключа; сырой ключ не храним
    owner       TEXT NOT NULL,
    rate_limit  INT NOT NULL DEFAULT 100,  -- запросов в минуту
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ
);

CREATE TABLE saved_reports (
    id          BIGSERIAL PRIMARY KEY,
    api_key_id  BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    params      JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_saved_reports_key ON saved_reports (api_key_id, created_at DESC);
```

Схема применяется автоматически при `docker compose up` одноразовым контейнером
`postgres-migrate` (`alembic upgrade head`, `services/pulse-api/alembic/`) — ручных шагов не
требуется.

## API (черновик контракта)

```
GET  /health                      — liveness/readiness, без ключа
GET  /api/v1/trending             — топ репозиториев по звёздам
       ?window=1h|24h|7d&language=python&limit=50&cursor=eyJv...
GET  /api/v1/repos/{owner}/{name} — карточка репозитория: активность, динамика
GET  /api/v1/languages/trends     — доли языков по времени
       ?window=30d&granularity=day
GET  /api/v1/activity/heatmap     — активность по часам/дням недели
GET  /api/v1/stats                — размер корпуса, лаг ingest, свежесть данных
POST /api/v1/reports              — сохранить отчёт
GET  /api/v1/reports/{id}         — выполнить сохранённый отчёт
GET  /metrics                     — Prometheus, без ключа
```

Все `/api/v1/*`-эндпоинты защищены API-ключом — заголовок `X-API-Key` (не `Authorization: Bearer`,
чтобы не намекать на OAuth/JWT, которых в проекте намеренно нет). `/health` и `/metrics` — открыты,
это операционные эндпоинты, не часть аналитического API. Rate limiting по ключу (лимит —
`api_keys.rate_limit`, окно 60 с, sliding window log в Redis) действует на все защищённые роуты;
`/trending` и `/languages/trends` дополнительно кэшируются в Redis (TTL 30 и 60 с соответственно,
заголовки `X-Cache: HIT|MISS`, `Cache-Control`, `ETag`) — детали реализации в `services/pulse-api/app/{auth,rate_limit,cache}.py`.

Все аналитические эндпоинты (`/trending`, `/repos/{owner}/{name}`, `/languages/trends`,
`/activity/heatmap`, `/stats`) поддерживают `If-None-Match` — совпавший `ETag` отвечает 304 без тела,
даже там, где нет Redis-кэша (`app/http_cache.py`). `/trending` — единственный по-настоящему
списочный эндпоинт контракта, поэтому курсорная пагинация (`?cursor=`, ответ несёт `next_cursor`)
реализована только на нём; keyset, не `OFFSET` — список читает горячий агрегат, который продолжает
меняться между запросами двух страниц одного клиента (`app/pagination.py`).

Требования: везде Pydantic-модели ответов, курсорная пагинация, structured logging с `trace_id`.

**Формат ошибок** (единый для всех эндпоинтов):

```json
{ "error": { "code": "rate_limited", "message": "Rate limit exceeded, retry in 12s" } }
```

Нераспознанный query-параметр — тоже ошибка, а не молчаливое игнорирование: любой параметр, не
объявленный сигнатурой эндпоинта, отвечает `422` в том же конверте (`code: "unknown_query_parameter"`),
одинаково на всех эндпоинтах, включая `/health` и `/metrics`. FastAPI по умолчанию просто отбросил бы
такой параметр и вернул `200` — конкретная ловушка на этом контракте: `/trending` понимает `?window=`,
а `/activity/heatmap` — нет, и клиент, скопировавший параметр между ними, тихо получил бы
неотфильтрованный ответ вместо ошибки. Известные параметры продолжают работать без изменений
(`services/pulse-api/app/query_params.py`).

## Осознанно не делаем

- **React/Vue-фронтенд** — Grafana-дашборд и/или минимальная страница на HTMX закрывают демо-нужды.
- **Дробление на дополнительные сервисы** — ровно два рантайм-сервиса (`gh-collector`, `pulse-api`)
  плюс консьюмер (`pulse-consumer`). Больше нет технической необходимости на этом масштабе.
- **Собственная аутентификация с JWT/OAuth и регистрацией** — для аналитического API с server-side
  ключами достаточно API-ключа.
- **LLM/AI-функциональность в продукте** — вне скоупа проекта; подробнее — [ADR 0005](adr/0005-portfolio-over-personal-tool.md).
- **Синтетические данные для бенчмарков** — GH Archive бесплатен и настоящий, нет причины их
  генерировать.

Каждое число производительности в этом репозитории трассируется к воспроизводимому прогону —
см. [docs/PERFORMANCE.md](PERFORMANCE.md).
