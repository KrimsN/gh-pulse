# Python — style guide

Этот документ описывает договорённости, которые не проверяет линтер: как раскладывать код по
модулям, как типизировать данные, как обрабатывать ошибки, как писать async-код и тесты. Механические
правила (длина строки, кавычки, импорты, docstring-конвенция) заданы в `pyproject.toml`
(`[tool.ruff]`, `[tool.mypy]`) — это источник истины, здесь они не дублируются. Применяется ко всем
Python-сервисам (`pulse-api`, `pulse-consumer`).

## 1. Именование и структура кода

### 1.1 Раскладка сервиса

Сервис растёт слоями по мере необходимости, а не заранее. Как только в одном файле смешивается
больше одной ответственности, разносим по пакетам — так `pulse-api` уже прошёл путь от одного
`main.py` до текущей раскладки:

```
app/
  config.py       # Settings, get_settings()
  main.py         # сборка приложения: structlog.configure, FastAPI(), lifespan, регистрация middleware и роутера
  middleware.py   # ASGI-middleware сервиса (TraceIdMiddleware, метрики запроса)
  helpers.py      # мелкие переиспользуемые хелперы уровня приложения, не привязанные к конкретному роуту
  api/            # роуты — тонкие, без бизнес-логики
  db/             # SQL-запросы и доступ к ClickHouse/PostgreSQL/Redis
  models/         # Pydantic-схемы запросов/ответов
  exceptions.py   # доменные исключения сервиса
```

Правило разделения: `api/` не знает SQL, `db/` не знает HTTP. Роут вызывает функцию из `db/`,
получает типизированный результат, отдаёт его наружу через Pydantic-модель.

`app` — модульная переменная в `main.py`; роуты в `api/` её не импортируют напрямую (это дало бы
циклический импорт: `main.py` импортирует `router` из `api/`, а `api/` импортировал бы `app` обратно
из `main.py`). Внутри обработчика состояние читается через параметр `request: Request` и
`request.app.state.*`, а не через захваченную глобальную `app`.

### 1.2 Именование

PEP 8 / pep8-naming уже enforced ruff-правилами `N*` —
[docs.astral.sh/ruff/rules/#pep8-naming-n](https://docs.astral.sh/ruff/rules/#pep8-naming-n).
Поверх этого:

- Функции доступа к данным — `verb_noun`: `get_trending_repos`, `insert_events_batch`,
  `fetch_hourly_stats`. Не `trending_repos()` (не понятно, что делает без чтения тела).
- Булевы переменные и поля — с `is_`/`has_`/`should_`: `is_healthy`, `has_more_pages`.
- ID сущностей — не голый `int`/`str`, а `NewType`, если ID разных сущностей могут перепутаться
  местами в сигнатуре (см. 2.1).

### 1.3 Публичный API модуля

`ruff` не требует `__all__` (`D104` и связанные выключены осознанно — докстринг не по умолчанию, а
когда действительно нужен). Но публичный API пакета — то, что импортируют снаружи, — обозначаем
явным `__all__` в `__init__.py`, если пакет реэкспортирует более одного символа. Это отличает
«внутреннее» от «то, на что можно опираться снаружи пакета».

### 1.4 SQL — не ORM

Аналитический SQL — суть проекта, ORM для него не используется. Практика:

- Запрос — это строка (или `.sql`-файл для длинных), а не собранный по кусочкам билдер.
- Каждая функция в `db/` — один запрос, один смысл. Не смешивать «получить» и «посчитать агрегат»
  в одной функции с флагом-параметром.
- Параметры — только через плейсхолдеры драйвера (`asyncpg`: `$1`, `$2`; `clickhouse-connect`:
  `parameters=`). Никогда не f-string в SQL с значениями из запроса — `S608` выключен в ruff именно
  потому, что raw SQL — легитимный паттерн здесь, а не потому, что инъекции не важны.

### 1.5 FastAPI, а не Starlette — где это возможно

FastAPI построен поверх Starlette и реэкспортирует большую часть публичного API (`Request`,
`Response`, `JSONResponse`, `status` и т. д.). Импортируем именно через `fastapi`/`fastapi.responses`
— один источник истины на проект, а не два пути к одному и тому же классу вперемешку. Импорт
напрямую из `starlette.*` — только там, где FastAPI ничего не реэкспортирует.

<details>
<summary>Что откуда импортировать</summary>

```python
# ХОРОШО — путь через fastapi
from fastapi import Request, status
from fastapi.responses import Response, JSONResponse

# ПЛОХО — тот же класс, но путь через starlette
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

# ИСКЛЮЧЕНИЕ: FastAPI не реэкспортирует ASGI-middleware база-классы —
# для собственного BaseHTTPMiddleware импорт из starlette легитимен (см. middleware.py)
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
```
</details>

### 1.6 HTTP-статусы — константы, не числа

`fastapi.status` (реэкспорт `starlette.status`) — именованные константы на каждый код. Голый
`200`/`503` в коде читается медленнее, чем `status.HTTP_200_OK`, и не подсвечивает опечатку в коде
(`503` вместо, скажем, `530` — оба «просто число», а `status.HTTP_530_...` не существует и упадёт на
импорте). `PLR2004` (magic-value-comparison) в ruff выключен осознанно — оставляет этот случай
человеку, здесь правило и фиксируется.

```python
# ПЛОХО
return JSONResponse(content=body, status_code=200 if healthy else 503)

# ХОРОШО
return JSONResponse(
    content=body,
    status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
)
```

### 1.7 Именованные аргументы — по умолчанию

Позиционные аргументы не подписаны на месте вызова: перепутать порядок легко, а линтер это не
ловит (типы часто совпадают или неявно приводятся). По умолчанию — именованные аргументы. Позиция
допустима, когда аргумент короткий и однозначно читается на месте (простой литерал, атрибут без
цепочки вызовов) — сложное выражение (вызов, цепочка методов, awaitable) подписываем ключом, даже
если оно единственное такое в вызове. Стиль можно смешивать внутри одного вызова: то, что коротко —
по позиции, то, что длинно или неочевидно — по ключу. Ещё один законный повод остаться на позиции —
общепризнанная конвенция самой библиотеки/языка, ломать её ради буквы правила не стоит.

<details>
<summary>Примеры</summary>

```python
# ПЛОХО — три однотипных строки/числа, порядок на месте вызова не виден
REQUEST_COUNT.labels(request.method, request.url.path, response.status_code).inc()

# ХОРОШО
REQUEST_COUNT.labels(method=request.method, path=request.url.path, status=response.status_code).inc()

# ПЛОХО — check прячется в позиции, хотя это целая цепочка вызовов
await probe_dependency("clickhouse", state.clickhouse.ping())

# ХОРОШО — name короткий и однозначный, остаётся по позиции; check — сложное выражение, по ключу
await probe_dependency("clickhouse", check=state.clickhouse.ping())

# ИСКЛЮЧЕНИЕ — общепризнанная конвенция: у структлога event всегда первым позиционным
logger.info("request_started")
logger.warning("dependency_check_degraded", dependency=name)

# ИСКЛЮЧЕНИЕ — общепризнанная конвенция драйверов БД: запрос всегда первым позиционным
await state.postgres.fetchval("SELECT 1")

# ИСКЛЮЧЕНИЕ — один параметр, перепутать не с чем
redis.Redis.from_url(settings.redis_url)
app.include_router(router)
```
</details>

## 2. Типизация и обработка ошибок

### 2.1 Чем описывать данные

<details>
<summary>Три инструмента, три случая — когда какой</summary>

```python
# Pydantic BaseModel — данные пересекают границу процесса
# (HTTP-запрос/ответ, то, что валидируется из внешнего мира)
class TrendingRepoResponse(BaseModel):
    repo_name: str
    stars_gained: int

# dataclass — внутренняя структура внутри процесса, границу не пересекает,
# валидация не нужна (данные уже пришли из БД типизированными)
@dataclass(frozen=True, slots=True)
class RepoStatsRow:
    repo_name: str
    stars_gained: int

# NewType — когда просто int/str смешивает несмешиваемое
RepoId = NewType("RepoId", int)
UserId = NewType("UserId", int)

def get_repo(repo_id: RepoId) -> RepoStatsRow: ...
```
</details>

Правило: `BaseModel` там, где нужна валидация или сериализация на границе процесса; `dataclass`
внутри процесса, где данные уже гарантированно корректны; `TypedDict` — только для интеропа с кодом,
которому нужен именно `dict` (например, kwargs в структурированный логгер).

### 2.2 `Any` и `# type: ignore`

`mypy --strict` запрещает неявный `Any`. Явный `Any` и `# type: ignore[код]` допустимы только с
комментарием, объясняющим, почему статически это не выразить (обычно — граница с нетипизированной
библиотекой). `# type: ignore` без кода ошибки в квадратных скобках не проходит ревью — он глушит
любую ошибку на строке, а не ту одну, которую вы проверили.

### 2.3 Валидация — только на границе

Принцип: не валидировать то, что не может случиться. На практике это значит — один слой валидации
на входе в систему.

<details>
<summary>Пример: где валидация нужна, а где — нет</summary>

```python
# ГРАНИЦА: HTTP-запрос — Pydantic валидирует автоматически на входе в роут
@app.get("/api/v1/trending")
async def trending(window: TrendingWindow) -> TrendingRepoResponse: ...

# ВНУТРИ: db/trending.py получает уже провалидированный TrendingWindow.
# Здесь НЕ нужно перепроверять, что window.hours > 0 — Pydantic это гарантировал выше.
async def fetch_trending(client: AsyncClient, window: TrendingWindow) -> list[RepoStatsRow]:
    ...
```
</details>

### 2.4 Исключения

Доменные исключения сервиса — в `app/exceptions.py`, наследуются от одного базового класса сервиса
(`class PulseApiError(Exception)`), не от голого `Exception` по отдельности. Это даёт один `except
PulseApiError` на границе (FastAPI exception handler), который переводит доменную ошибку в HTTP-
ответ, — и не даёт доменной ошибке случайно утечь наружу как 500 без структурированного тела.

Общих/переиспользуемых между сервисами исключений не заводим, пока не появится второй сервис,
которому реально нужен тот же класс ошибки, — раньше это гадание на будущее.

### 2.5 Чего не делать

- Не оборачивать в `try/except` то, что физически не может бросить — обратная сторона того же
  принципа, что и валидация только на границе (см. 2.3). `except Exception: pass` ради «на всякий
  случай» не проходит ревью — молча проглоченная ошибка дороже, чем упавший процесс с трейсбеком.
- Голый `except:` (без класса) запрещён `mypy`/`ruff` (`E722`, `BLE001`) — это уже enforced.

## 3. Async, ресурсы и производительность

### 3.1 Никаких блокирующих вызовов в async-коде

<details>
<summary>Плохо / хорошо</summary>

```python
# ПЛОХО: requests — синхронный, блокирует event loop всего процесса
async def fetch_repo(name: str) -> dict:
    return requests.get(f"https://api.github.com/repos/{name}").json()

# ХОРОШО: httpx.AsyncClient — не блокирует event loop
async def fetch_repo(client: httpx.AsyncClient, name: str) -> dict:
    response = await client.get(f"https://api.github.com/repos/{name}")
    return response.json()
```
</details>

Это касается и `time.sleep` (→ `asyncio.sleep`), и любых sync-драйверов БД. Проект уже стоит на
async-стеке (`asyncpg`, `clickhouse-connect[async]`, `redis.asyncio`) — синхронный клиент к тем же
сторам в async-функции не подключаем никогда, даже «на скорую руку».

### 3.2 Управление ресурсами

Канонический паттерн уже есть в [main.py](../services/pulse-api/app/main.py) — пулы
(`clickhouse`, `postgres`, `redis`) создаются один раз в `lifespan` и живут в `app.state`, закрываются
в `finally`. Новый сервис, новый клиент стороннего API — тот же паттерн: создание пула/клиента в
одном месте при старте, явное закрытие при остановке, никаких соединений, открываемых по одному на
запрос.

### 3.3 Батчинг вместо N+1

`pulse-consumer` вставляет события пачками, не по одному — это не оптимизация «на будущее», а
единственный режим, в котором ClickHouse рассчитан работать
([ClickHouse best practices](https://clickhouse.com/docs/en/optimize/bulk-inserts)). Практика: если
цикл делает `await` внутри `for` по элементам одного источника — это повод спросить, нельзя ли
собрать все элементы и отправить одним batch-запросом (`executemany`, `insert_many` и аналоги).

### 3.4 Ограниченная конкурентность

<details>
<summary>Пример: gather без ограничения — риск; с Semaphore — контролируемо</summary>

```python
# РИСК: если repos — 10 000 элементов, это 10 000 одновременных соединений
results = await asyncio.gather(*(fetch_repo(client, r) for r in repos))

# КОНТРОЛИРУЕМО: не больше 20 одновременных запросов
semaphore = asyncio.Semaphore(20)

async def bounded_fetch(client: httpx.AsyncClient, name: str) -> dict:
    async with semaphore:
        return await fetch_repo(client, name)

results = await asyncio.gather(*(bounded_fetch(client, r) for r in repos))
```
</details>

`asyncio.gather` без ограничения допустим только когда размер коллекции заведомо мал и известен
заранее (единицы, не «сколько прилетит из API»).

## 4. Тестирование и логирование

### 4.1 Тесты — против реальных сторов

`pytest` + `testcontainers` против реальных ClickHouse/PostgreSQL/Kafka, моков для датасторов нет.
Настройка контейнеров и фикстур —
[testcontainers-python docs](https://testcontainers-python.readthedocs.io/).

### 4.2 Структура теста

Arrange-Act-Assert, разделённые пустой строкой — без явных комментариев `# arrange` и т. д., пустая
строка уже маркирует границу:

<details>
<summary>Пример</summary>

```python
async def test_fetch_trending_returns_repos_sorted_by_stars(clickhouse_client):
    await insert_test_events(clickhouse_client, events=[...])

    result = await fetch_trending(clickhouse_client, window=TrendingWindow(hours=24))

    assert [r.repo_name for r in result] == ["repo-b", "repo-a"]
```
</details>

### 4.3 Именование тестов

`test_<что_проверяем>_<при_каком_условии>` — имя теста должно быть читаемо как описание поведения
без открытия тела функции: `test_health_returns_503_when_clickhouse_down`, а не
`test_health_2`.

### 4.4 Логирование — событийные имена и поля

Конвенция уже введена [ADR 0006](adr/0006-structlog-for-logging.md), `main.py` и `middleware.py`:

- Имя события — `snake_case`, глагол в прошедшем времени или существительное-факт:
  `request_started`, `dependency_check_failed`. Не предложение и не f-string с интерполяцией —
  весь контекст идёт отдельными kwargs (`logger.info("request_finished", status_code=..., duration_ms=...)`),
  чтобы поля были агрегируемы в JSON, а не склеены в текст.
- Request-scoped контекст (`trace_id`, `path`, `method`) — только через
  `structlog.contextvars.bind_contextvars`, не передаётся вручную параметром через цепочку вызовов.

### 4.5 Метки метрик — шаблон роута, а не путь запроса

Label в Prometheus заводит отдельную time series на каждое уникальное значение. Поэтому в метку пути
идёт **шаблон** роута (`/api/v1/repos/{owner}/{name}`), а не фактический путь
(`/api/v1/repos/torvalds/linux`): иначе кардинальность растёт с числом запросов, а не с числом роутов,
и Prometheus ложится на первом же параметризованном эндпоинте. Готовая реализация — `_route_label` в
[middleware.py](../services/pulse-api/app/middleware.py), новый сервис берёт её оттуда.

Две ловушки, которые она закрывает и которые неочевидны по документации FastAPI:

- **Префикс `include_router` не попадает в `route.path_format`.** Для роута, подключённого с
  `prefix="/api/v1"`, `scope["route"].path_format` вернёт `/repos/{owner}/{name}` — без префикса.
  Эффективный путь лежит только в приватном `scope["fastapi"]["effective_route_context"]`, поэтому
  спрашиваем сначала его, а `scope["route"]` держим фолбэком: при изменении приватного API метка
  потеряет префикс, но останется шаблоном.
- **`scope["route"]` отсутствует на 404** — обращаться только через `.get()`, иначе несовпавший путь
  роняет middleware. Такие запросы идут под меткой `unmatched`, единой на все несуществующие пути.

### 4.6 Чего не логировать

- Значения внутри батч-циклов поэлементно (см. 3.3) — один `logger.info("batch_inserted",
  count=len(batch))` после цикла, не `count` записей `logger.info` внутри него.
- Сырые тела запросов/ответов и содержимое `postgres_dsn`/токенов — секреты и PII в структурированный
  JSON-лог не попадают, даже в `debug`-уровне.

## Смотрите также

- `pyproject.toml` — механические правила (`ruff`, `mypy`), источник истины для форматирования и
  линтинга.
- [docs/adr/](adr/) — обоснование конкретных технических решений (ClickHouse, structlog и т. д.).
