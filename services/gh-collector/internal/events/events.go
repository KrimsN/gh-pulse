// Package events поллит GitHub Events API (GET https://api.github.com/events) — источник событий
// "прямо сейчас" в отличие от internal/archive, который качает уже готовые почасовые дампы. Оба
// пакета отдают один и тот же model.Event через один и тот же model.ParseGitHubEvent — маппинг
// сырого события GitHub решается только там, здесь его не дублируем (см. её доккомментарий).
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

const (
	// defaultBaseURL — адрес Events API.
	defaultBaseURL = "https://api.github.com/events"

	// perPage — сколько событий просить на странице. Черновик задачи 2.9 предполагал 300 (лимит
	// из документации GitHub для других списковых эндпоинтов), но реальный ответ (проверено живым
	// запросом per_page=300 перед тем, как писать этот пакет) всё равно возвращает ровно 100
	// событий — GitHub молча обрезает per_page для /events, без ошибки и без предупреждения в
	// заголовках. 100 здесь — не наша граница, а этот же реальный потолок, без иллюзии, что можно
	// попросить больше.
	perPage = 100

	// maxResponseBytes — верхняя граница размера тела ответа, которую мы соглашаемся прочитать
	// целиком в память. В отличие от internal/archive, где io.ReadAll на весь час запрещён (там
	// это гигабайты), здесь io.ReadAll оправдан: страница Events API — конечный маленький объект,
	// а не гигабайтный поток. Реальная страница из 100 событий (см.
	// testdata/sample_events.json — вырезка из настоящего ответа) весит 60-100 КБ. 4 МБ — почти на
	// два порядка больше, тот же запас и та же логика, что у maxLineSize в internal/archive: тело
	// такого размера означает не "легитимно большая страница", а порчу или аномалию ответа, и
	// трактуется как фатальный сбой этой страницы, а не повод растить лимит дальше.
	//
	// Это же и есть та самая осознанная граница на PayloadSize из model.ParseGitHubEvent (см. её
	// доккомментарий про небезопасность uint32(payloadLen) без явного лимита источника): payload
	// одного события — подстрока всего тела ответа, так что maxResponseBytes ограничивает его
	// сверху транзитивно, с огромным запасом до потолка uint32 (~4.29e9 байт).
	maxResponseBytes = 4 * 1024 * 1024

	// defaultPollInterval — темп поллинга, если сервер не прислал X-Poll-Interval вовсе. В
	// реальных ответах он есть всегда (проверено), но полагаться на это молча — то же допущение,
	// которое подвело бы, если GitHub когда-нибудь уберёт заголовок. 60с совпадает с тем, что
	// реально отдаёт GitHub на момент написания этого пакета.
	defaultPollInterval = 60 * time.Second

	// retryInterval — пауза перед повтором после сбоя одного Poll (сеть, неожиданный статус,
	// битый ответ). Не связана с rate-limit backoff (см. backoffInterval) — отдельная защита от
	// busy-loop на постоянно недоступных сети/сервере.
	retryInterval = 10 * time.Second

	// rateLimitResetMargin — запас поверх X-RateLimit-Reset, на который Run досыпает при
	// исчерпанном лимите: часы этого процесса и GitHub не гарантированно синхронизированы день в
	// день, и без запаса легко проснуться на секунду раньше настоящего сброса и снова словить 403.
	rateLimitResetMargin = 2 * time.Second

	// dedupWindow — сколько последних event_id помнит Run для локального дедупа перекрывающихся
	// страниц (см. её доккомментарий и seenIDs). Вдвое больше страницы: если бы окно совпадало со
	// страницей один-в-один, событие с самого начала предыдущей страницы, всё ещё встречающееся в
	// начале следующей (поток на границе страниц сдвигается не строго на всю страницу), выпало бы
	// из окна раньше, чем перестало повторяться.
	dedupWindow = 2 * perPage
)

// Metrics — Prometheus-метрики пакета. Отдельная структура и nil-safe инкременты — по тем же
// причинам, что и у internal/archive.Metrics и internal/producer.Metrics: явная инъекция вместо
// общего DefaultRegisterer, чтобы тесты и несколько независимых Client в одном процессе не
// толкались за регистрацию одних и тех же имён.
type Metrics struct {
	PollErrors         prometheus.Counter
	EventsSkipped      prometheus.Counter
	NotModified        prometheus.Counter
	DuplicatesSkipped  prometheus.Counter
	RateLimitRemaining prometheus.Gauge
}

// NewMetrics создаёт и регистрирует метрики пакета в reg. RateLimitRemaining — тот самый
// gh_collector_github_rate_limit_remaining, который до этой задачи был захардкожен нулём прямо в
// cmd/gh-collector/main.go (заглушка "метрика ещё не подключена"): здесь он становится настоящим
// Gauge, который Poll обновляет на каждом ответе.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		PollErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_events_poll_errors_total",
			Help: "Число неуспешных запросов к GitHub Events API (сеть, неожиданный HTTP-статус, " +
				"битый или слишком большой ответ). Один сбой не останавливает live-поллинг целиком — см. Client.Run.",
		}),
		EventsSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_events_skipped_total",
			Help: "Число событий Events API, пропущенных как битые внутри иначе валидной страницы " +
				"(аналог gh_collector_lines_skipped_total у GH Archive).",
		}),
		NotModified: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_events_not_modified_total",
			Help: "Число ответов 304 Not Modified. Такие запросы не расходуют X-RateLimit-Remaining " +
				"(критерий приёмки задачи 2.9) — рост этого счётчика без соответствующего расхода лимита это и подтверждает.",
		}),
		DuplicatesSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_events_deduplicated_total",
			Help: "Число событий, отброшенных локальным дедупом соседних перекрывающихся страниц Events API. " +
				"Не гарантия дедупликации — та обеспечена на уровне ClickHouse (ADR 0004), это лишь снижение нагрузки на Kafka.",
		}),
		RateLimitRemaining: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gh_collector_github_rate_limit_remaining",
			Help: "Последнее увиденное значение X-RateLimit-Remaining GitHub Events API.",
		}),
	}
	reg.MustRegister(m.PollErrors, m.EventsSkipped, m.NotModified, m.DuplicatesSkipped, m.RateLimitRemaining)
	return m
}

// incPollErrors, incEventsSkipped, incNotModified, incDuplicatesSkipped, setRateLimitRemaining —
// nil-safe операции: тем же приёмом, что и в internal/archive.Metrics (метод на нулевом указателе
// безопасен, пока не разыменовывает поля), тестам, которым метрики не важны, не нужно поднимать Registry.
func (m *Metrics) incPollErrors() {
	if m == nil {
		return
	}
	m.PollErrors.Inc()
}

func (m *Metrics) incEventsSkipped() {
	if m == nil {
		return
	}
	m.EventsSkipped.Inc()
}

func (m *Metrics) incNotModified() {
	if m == nil {
		return
	}
	m.NotModified.Inc()
}

func (m *Metrics) incDuplicatesSkipped() {
	if m == nil {
		return
	}
	m.DuplicatesSkipped.Inc()
}

func (m *Metrics) setRateLimitRemaining(v int) {
	if m == nil {
		return
	}
	m.RateLimitRemaining.Set(float64(v))
}

// Client поллит GitHub Events API. Как и archive.Client и producer.Producer, это структура, а не
// пакетные глобалы — то же обоснование (несколько независимых экземпляров в тестах, разделяемое, но
// явно переданное состояние).
//
// В отличие от archive.Client, здесь есть по-настоящему мутируемое поле — etag, обновляемое между
// вызовами Poll. Из-за этого один Client НЕ безопасен для конкурентных вызовов Poll/Run из
// нескольких горутин одновременно: он рассчитан ровно на один активный цикл поллинга (Run гонит его
// последовательно), и это осознанный выбор — GitHub всё равно отдаёт единый общий поток /events, так
// что параллельный поллинг одним и тем же клиентом не даёт дополнительных данных, только гонку за etag.
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
	logger     *log.Logger
	metrics    *Metrics
	etag       string // ETag последнего успешного (200) ответа; пусто, пока не было ни одного
}

// NewClient строит Client. token может быть пустой строкой — тогда запросы идут
// неаутентифицированными (лимит 60 запросов/час вместо 5000/час, см. GITHUB_TOKEN в «Сквозных
// соглашениях»). metrics и logger можно передать nil — то же поведение, что у archive.NewClient.
func NewClient(token string, metrics *Metrics, logger *log.Logger) *Client {
	if logger == nil {
		logger = log.Default()
	}
	return &Client{
		baseURL:    defaultBaseURL,
		httpClient: newHTTPClient(),
		token:      token,
		logger:     logger,
		metrics:    metrics,
	}
}

// newHTTPClient строит *http.Client с общим Timeout — в отличие от archive.newHTTPClient, где он
// намеренно отсутствует (час GH Archive легально качается минутами). Ответы Events API маленькие
// (см. maxResponseBytes) и опрашиваются часто: если сеть однажды не ответит вовсе, один зависший
// запрос не должен блокировать poll-цикл на неопределённое время, пока X-Poll-Interval и так велит
// ждать секунды, а не минуты, между попытками.
func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
	}
}

// PollResult — результат одного запроса Poll: события (пусто при 304) плюс метаданные заголовков,
// нужные Run для темпа поллинга и backoff.
//
// RateLimitRemaining и RateLimitReset заполняются по возможности, даже когда Poll вернула error:
// GitHub отдаёт эти заголовки на любом статусе с заголовками вообще, включая неожиданный (403,
// 5xx) — не только на 200/304. Run использует эти best-effort значения, чтобы посчитать паузу перед
// повтором и на пути ошибки тоже (см. её доккомментарий), а не только в штатном случае. Единственный
// путь, где заголовков нет вовсе, — сбой на уровне транспорта (сеть недоступна, соединение не
// установилось): тогда PollResult остаётся нулевым значением, и backoffInterval деградирует к
// минимальной паузе (см. её доккомментарий про remaining<=0 и нулевое время сброса).
type PollResult struct {
	Events             []model.Event
	NotModified        bool
	PollInterval       time.Duration
	RateLimitRemaining int
	RateLimitReset     time.Time
}

// Poll делает один запрос к Events API: GET /events?per_page=100 с If-None-Match (если есть ETag от
// прошлого успешного 200-ответа) и Authorization (если задан токен). Обновляет метрику
// RateLimitRemaining по факту любого ответа с заголовками, включая 304 — тела 304 не несёт, но
// заголовки rate-limit отдаёт наравне с 200 (проверено на реальном ответе, см. testdata).
//
// Возвращает ошибку на сетевом сбое, неожиданном HTTP-статусе (не 200 и не 304) или невалидном теле
// 200-ответа (см. decodeEvents). Единичное битое событие внутри иначе валидной страницы не входит в
// их число — оно логируется, считается в EventsSkipped и пропускается, страница разбирается дальше
// (тот же принцип, что у archive.decodeLines для одной битой строки JSON Lines).
func (c *Client) Poll(ctx context.Context) (PollResult, error) {
	url := fmt.Sprintf("%s?per_page=%d", c.baseURL, perPage)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return PollResult{}, fmt.Errorf("build request for %s: %w", url, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	// User-Agent — общее требование GitHub REST API: часть эндпоинтов без него отвечает 403 без
	// объяснения причины в теле, и ни один из остальных заголовков этого не покрывает.
	req.Header.Set("User-Agent", "gh-pulse-collector")
	if c.etag != "" {
		req.Header.Set("If-None-Match", c.etag)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.metrics.incPollErrors()
		return PollResult{}, fmt.Errorf("poll %s: %w", url, err)
	}
	// Ошибку закрытия глушим осознанно — то же обоснование, что и в archive.Client.FetchHour: тело
	// только читается, и его Close сообщает лишь о сбое возврата соединения в пул.
	defer func() { _ = resp.Body.Close() }()

	result := PollResult{
		PollInterval:       parsePollInterval(resp.Header.Get("X-Poll-Interval"), c.logger),
		RateLimitRemaining: parseRateLimitRemaining(resp.Header.Get("X-RateLimit-Remaining")),
		RateLimitReset:     parseRateLimitReset(resp.Header.Get("X-RateLimit-Reset")),
	}
	c.metrics.setRateLimitRemaining(result.RateLimitRemaining)

	switch resp.StatusCode {
	case http.StatusNotModified:
		result.NotModified = true
		c.metrics.incNotModified()
		return result, nil

	case http.StatusOK:
		// ETag сохраняем только на успешном 200: 304 тела не несёт, и его ETag (если он вообще
		// присутствует в ответе) описывает тот же ресурс с той же меткой — менять c.etag не на что.
		if etag := resp.Header.Get("ETag"); etag != "" {
			c.etag = etag
		}
		evts, err := c.decodeEvents(resp.Body, url)
		if err != nil {
			c.metrics.incPollErrors()
			return result, err
		}
		result.Events = evts
		return result, nil

	default:
		c.metrics.incPollErrors()
		return result, fmt.Errorf("poll %s: unexpected status %s", url, resp.Status)
	}
}

// decodeEvents читает и разбирает тело успешного (200) ответа: JSON-массив сырых событий в той же
// форме, что и объекты GH Archive (см. model.ParseGitHubEvent — маппинг общий, не дублируется).
//
// io.LimitReader(body, maxResponseBytes+1), а не resp.Body напрямую: +1 байт сверх лимита — это не
// опечатка, а способ отличить "тело ровно maxResponseBytes байт" от "тело больше limit'а, но
// io.ReadAll просто остановился на границе, не пожаловавшись" — если после чтения данных больше
// maxResponseBytes, значит настоящее тело было длиннее лимита, и это трактуется как фатальный сбой
// страницы (см. доккомментарий maxResponseBytes), а не как повод молча обрезать её до заявленной
// длины и парсить обрубок.
func (c *Client) decodeEvents(body io.Reader, label string) ([]model.Event, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read response body for %s: %w", label, err)
	}
	if len(data) > maxResponseBytes {
		return nil, fmt.Errorf("response body for %s exceeds %d bytes (maxResponseBytes)", label, maxResponseBytes)
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode events array for %s: %w", label, err)
	}

	events := make([]model.Event, 0, len(raw))
	for i, r := range raw {
		evt, err := model.ParseGitHubEvent(r)
		if err != nil {
			c.metrics.incEventsSkipped()
			c.logger.Printf("events: ответ %s, элемент %d: пропускаю битое событие: %v", label, i, err)
			continue
		}
		events = append(events, evt)
	}
	return events, nil
}

// parsePollInterval разбирает X-Poll-Interval; при отсутствии или невалидном значении возвращает
// defaultPollInterval, залогировав это как аномалию (в реальных ответах заголовок есть всегда).
func parsePollInterval(raw string, logger *log.Logger) time.Duration {
	if raw == "" {
		return defaultPollInterval
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		logger.Printf("events: не удалось разобрать X-Poll-Interval=%q, использую %s по умолчанию", raw, defaultPollInterval)
		return defaultPollInterval
	}
	return time.Duration(seconds) * time.Second
}

// parseRateLimitRemaining разбирает X-RateLimit-Remaining; при отсутствии или невалидном значении
// возвращает 0 — тот же эффект, что и у настоящего "лимит исчерпан" в backoffInterval (см. её
// доккомментарий про remaining<=0), что безопаснее, чем притвориться, будто лимита в достатке.
func parseRateLimitRemaining(raw string) int {
	remaining, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return remaining
}

// parseRateLimitReset разбирает X-RateLimit-Reset (unix-время в секундах); при отсутствии или
// невалидном значении возвращает нулевое time.Time.
func parseRateLimitReset(raw string) time.Time {
	unixSeconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(unixSeconds, 0)
}

// backoffInterval вычисляет паузу, которую Run обязан выдержать сверх X-Poll-Interval, если
// оставшийся запас запросов рискует не дожить до X-RateLimit-Reset на текущем темпе.
//
// Идея — поделить время до сброса поровну на оставшиеся запросы. Пока запас большой (5000 в час у
// аутентифицированного клиента), результат — доли секунды, заведомо меньше X-Poll-Interval (обычно
// 60с), и в max(pollInterval, backoff) (см. Run) побеждает рекомендация GitHub, а не эта формула.
// Backoff перехватывает управление только когда remaining становится действительно небольшим
// относительно времени до сброса — то есть именно тогда, когда "приближение к лимиту" должно
// тормозить, а не раньше и не позже.
//
// remaining<=0 — отдельный случай: лимит уже исчерпан (или ещё неизвестен, см. доккомментарий
// PollResult про best-effort RateLimit* при ошибке Poll) — ждём буквально до reset с небольшим
// запасом (rateLimitResetMargin), а не пытаемся поделить время на ноль оставшихся запросов.
func backoffInterval(remaining int, reset, now time.Time) time.Duration {
	untilReset := reset.Sub(now)
	if remaining <= 0 {
		if untilReset < 0 {
			return rateLimitResetMargin
		}
		return untilReset + rateLimitResetMargin
	}
	if untilReset <= 0 {
		return 0
	}
	return untilReset / time.Duration(remaining)
}

// seenIDs — bounded множество последних увиденных event_id: дешёвый локальный дедуп перекрывающихся
// страниц Events API (соседние поллы могут вернуть одно и то же событие — GitHub не гарантирует
// страницам не пересекаться). НЕ гарантия дедупликации ниже по потоку — та обеспечена на уровне
// ClickHouse (ADR 0004); здесь только отсечение самых частых повторов, чтобы не грузить Kafka
// повторной продюсацией одного и того же event_id на каждом следующем полле.
//
// Держит настоящее множество, а не "id > last_max_id": id событий в ответе не монотонны внутри
// страницы (проверено на реальном ответе — например, три подряд id вида 15295311143, 15295311160,
// 15295311011, где третий меньше первых двух), так что простое сравнение с максимумом пропустило бы
// повторы или, того хуже, отбросило бы валидные более новые события с меньшим числовым id.
type seenIDs struct {
	limit int
	order []uint64 // порядок вставки — для вытеснения самых старых при переполнении
	set   map[uint64]bool
}

func newSeenIDs(limit int) *seenIDs {
	return &seenIDs{
		limit: limit,
		set:   make(map[uint64]bool, limit),
	}
}

// seenOrAdd возвращает true, если id уже встречался в окне (и в этом случае НЕ трогает порядок
// вытеснения — повторное попадание одного и того же id не должно продлевать его жизнь в окне сильнее,
// чем однократное). Иначе запоминает id и вытесняет самый старый, если окно переполнено.
func (s *seenIDs) seenOrAdd(id uint64) bool {
	if s.set[id] {
		return true
	}
	s.set[id] = true
	s.order = append(s.order, id)
	if len(s.order) > s.limit {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.set, oldest)
	}
	return false
}

// sleepCtx ждёт d, либо возвращается раньше, если ctx отменён. Возвращает false во втором случае —
// вызывающий код (Run) читает это как штатную остановку (SIGINT/SIGTERM), а не как окончание паузы.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// Run поллит Events API в бесконечном цикле, пока не отменён ctx, отправляя нормализованные события
// в out. out — тот же канал с backpressure, что и в archive.Client.FetchHour (см. её доккомментарий
// про семантику out): если консьюмер (продюсер Kafka в cmd/gh-collector) не успевает вычитывать
// события, отправка в out блокируется, и Run просто перестаёт запрашивать API быстрее, чем события
// успевают уйти дальше — никакой внутренней неограниченной буферизации. Run не закрывает out по той
// же причине, что и FetchHour: закрытие остаётся на вызывающем коде, который точно знает, что
// поллинг закончился.
//
// Возвращает ctx.Err() при штатной остановке — тем же способом, каким об этом сигнализирует
// archive.Client.FetchHour, чтобы вызывающий код мог одинаково отличать отмену от сбоя через
// errors.Is(err, context.Canceled) в обоих режимах (backfill и live).
//
// Единичный сбой Poll не останавливает live-режим целиком. В отличие от одного часа GH Archive, где
// сбой воркера errgroup — законная фатальная ошибка (час либо докачан, либо нет, третьего не дано),
// здесь "час" не кончается никогда, и уронить весь процесс из-за одного плохого ответа означало бы
// требовать внешнего перезапуска на каждый временный сетевой сбой. Вместо этого ошибка логируется,
// считается в Metrics.PollErrors (см. Poll) и цикл продолжает поллинг после паузы — retryInterval
// либо, если сбой сопровождался заголовками rate-limit (см. доккомментарий PollResult), большее из
// retryInterval и backoffInterval.
func (c *Client) Run(ctx context.Context, out chan<- model.Event) error {
	seen := newSeenIDs(dedupWindow)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		result, err := c.Poll(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			c.logger.Printf("events: poll: %v", err)
			wait := retryInterval
			if backoff := backoffInterval(result.RateLimitRemaining, result.RateLimitReset, time.Now()); backoff > wait {
				wait = backoff
			}
			if !sleepCtx(ctx, wait) {
				return ctx.Err()
			}
			continue
		}

		for _, evt := range result.Events {
			if seen.seenOrAdd(evt.EventID) {
				c.metrics.incDuplicatesSkipped()
				continue
			}
			select {
			case out <- evt:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		wait := result.PollInterval
		if backoff := backoffInterval(result.RateLimitRemaining, result.RateLimitReset, time.Now()); backoff > wait {
			wait = backoff
		}
		if !sleepCtx(ctx, wait) {
			return ctx.Err()
		}
	}
}
