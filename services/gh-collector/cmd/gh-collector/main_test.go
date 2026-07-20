package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/producer"
)

// TestEnvOr — задача 2.12: envOr не была покрыта ни разу. os.LookupEnv (а не os.Getenv) различает
// «переменная не задана» и «задана пустой строкой» — оба случая табличного теста ниже проверяют
// именно эту границу, а не просто «работает».
func TestEnvOr(t *testing.T) {
	const key = "GH_COLLECTOR_TEST_ENV_OR"

	t.Run("переменная не задана — возвращается fallback", func(t *testing.T) {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("Unsetenv: %v", err)
		}
		if got := envOr(key, "fallback"); got != "fallback" {
			t.Errorf("envOr() = %q, want %q", got, "fallback")
		}
	})

	t.Run("переменная задана пустой строкой — возвращается пустая строка, не fallback", func(t *testing.T) {
		t.Setenv(key, "")
		if got := envOr(key, "fallback"); got != "" {
			t.Errorf("envOr() = %q, want \"\" — пустая строка это осознанное значение, не повод подставлять дефолт", got)
		}
	})

	t.Run("переменная задана — возвращается её значение", func(t *testing.T) {
		t.Setenv(key, "actual")
		if got := envOr(key, "fallback"); got != "actual" {
			t.Errorf("envOr() = %q, want %q", got, "actual")
		}
	})
}

// TestMetricsHandler — задача 2.12: metricsHandler не была покрыта ни разу. Проверяет, что
// обработчик реально отдаёт метрики зарегистрированного Registry в text-формате Prometheus, а не
// просто что он не паникует.
func TestMetricsHandler(t *testing.T) {
	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "gh_collector_test_metrics_handler_total"})
	counter.Inc()
	reg.MustRegister(counter)

	handler := metricsHandler(reg)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "gh_collector_test_metrics_handler_total 1") {
		t.Errorf("тело /metrics не содержит зарегистрированную метрику:\n%s", rec.Body.String())
	}
}

// TestParseHour — табличный тест на разбор --hour. Отдельно фиксирует случаи, из-за которых
// прежняя реализация на fmt.Sscanf + time.Date молча качала не тот час вместо ошибки:
// нераспознанный хвост, месяц/день/час вне диапазона — все обязаны стать ошибкой, а не тихой
// нормализацией на другую дату.
func TestParseHour(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "обычный час с двузначным часом",
			input: "2026-06-01-15",
			want:  time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
		},
		{
			name:  "час без ведущего нуля",
			input: "2026-06-01-9",
			want:  time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC),
		},
		{
			name:  "час 0 — полночь",
			input: "2026-01-05-0",
			want:  time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "час 23 — последний час суток, граница",
			input: "2026-01-05-23",
			want:  time.Date(2026, 1, 5, 23, 0, 0, 0, time.UTC),
		},
		{
			name:    "хвост после часа — раньше проглатывался Sscanf",
			input:   "2026-06-01-15-garbage",
			wantErr: true,
		},
		{
			name:    "месяц и день вне диапазона — раньше time.Date молча уезжал на 2027-02-18",
			input:   "2026-13-45-99",
			wantErr: true,
		},
		{
			name:    "час 24 — раньше молча переносился на следующий день, час 0",
			input:   "2026-06-01-24",
			wantErr: true,
		},
		{
			name:    "месяц и день 0 — раньше молча уезжал на 2025-11-30",
			input:   "2026-00-00-0",
			wantErr: true,
		},
		{
			name:    "отрицательный час",
			input:   "2026-06-01--1",
			wantErr: true,
		},
		{
			name:    "меньше четырёх полей",
			input:   "2026-06-01",
			wantErr: true,
		},
		{
			name:    "час не число",
			input:   "2026-06-01-abc",
			wantErr: true,
		},
		{
			name:    "пустая строка",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHour(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("ожидалась ошибка, получено %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("parseHour(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestExtractBackfillRange проверяет вырезание "--backfill ОТ ДО" из args до flag.Parse — в том
// числе ключевой случай из примера CLI задачи 1.4, где --workers идёт ПОСЛЕ позиционных ОТ/ДО
// (см. доккомментарий extractBackfillRange о том, почему это вообще нужно писать руками).
func TestExtractBackfillRange(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantFrom      string
		wantTo        string
		wantRemainder []string
		wantFound     bool
		wantErr       bool
	}{
		{
			name:          "--backfill в начале, флаги после диапазона — пример CLI из задачи 1.4",
			args:          []string{"--backfill", "2026-06-01-0", "2026-06-02-0", "--workers", "8"},
			wantFrom:      "2026-06-01-0",
			wantTo:        "2026-06-02-0",
			wantRemainder: []string{"--workers", "8"},
			wantFound:     true,
		},
		{
			name:          "--backfill после других флагов",
			args:          []string{"--workers", "4", "--backfill", "2026-06-01-0", "2026-06-02-0"},
			wantFrom:      "2026-06-01-0",
			wantTo:        "2026-06-02-0",
			wantRemainder: []string{"--workers", "4"},
			wantFound:     true,
		},
		{
			name:          "--hour без --backfill — found=false, args не тронуты",
			args:          []string{"--hour", "2026-06-01-15"},
			wantRemainder: []string{"--hour", "2026-06-01-15"},
			wantFound:     false,
		},
		{
			name:      "--backfill без аргументов — ошибка использования",
			args:      []string{"--backfill"},
			wantFound: true,
			wantErr:   true,
		},
		{
			name:      "--backfill только с одним аргументом — ошибка использования",
			args:      []string{"--backfill", "2026-06-01-0"},
			wantFound: true,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, to, remainder, found, err := extractBackfillRange(tt.args)

			if tt.wantErr {
				if !errors.Is(err, errUsage) {
					t.Fatalf("ожидалась ошибка, оборачивающая errUsage, получено %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if found != tt.wantFound {
				t.Errorf("found = %v, want %v", found, tt.wantFound)
			}
			if !tt.wantFound {
				return
			}
			if from != tt.wantFrom || to != tt.wantTo {
				t.Errorf("from=%q to=%q, want from=%q to=%q", from, to, tt.wantFrom, tt.wantTo)
			}
			if len(remainder) != len(tt.wantRemainder) {
				t.Fatalf("remainder=%v, want %v", remainder, tt.wantRemainder)
			}
			for i := range remainder {
				if remainder[i] != tt.wantRemainder[i] {
					t.Errorf("remainder=%v, want %v", remainder, tt.wantRemainder)
				}
			}
		})
	}
}

// TestResolveHours проверяет генерацию списка часов из --hour/--backfill, в частности что верхняя
// граница --backfill исключающая (см. доккомментарий resolveHours): пример из задачи 1.4
// (2026-06-01-0 .. 2026-06-02-0) обязан дать ровно 24 часа, а не 25.
func TestResolveHours(t *testing.T) {
	t.Run("--hour: один час", func(t *testing.T) {
		got, err := resolveHours(false, "", "", "2026-06-01-15")
		if err != nil {
			t.Fatalf("неожиданная ошибка: %v", err)
		}
		want := []time.Time{time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)}
		if len(got) != 1 || !got[0].Equal(want[0]) {
			t.Errorf("resolveHours = %v, want %v", got, want)
		}
	})

	t.Run("--backfill: верхняя граница исключающая, ровно 24 часа на сутки", func(t *testing.T) {
		got, err := resolveHours(true, "2026-06-01-0", "2026-06-02-0", "")
		if err != nil {
			t.Fatalf("неожиданная ошибка: %v", err)
		}
		if len(got) != 24 {
			t.Fatalf("resolveHours вернул %d часов, ожидалось 24", len(got))
		}
		if !got[0].Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("первый час = %v, want 2026-06-01-0", got[0])
		}
		if !got[23].Equal(time.Date(2026, 6, 1, 23, 0, 0, 0, time.UTC)) {
			t.Errorf("последний час = %v, want 2026-06-01-23 (2026-06-02-0 — исключающая граница)", got[23])
		}
	})

	t.Run("--backfill: диапазон в один час", func(t *testing.T) {
		got, err := resolveHours(true, "2026-06-01-0", "2026-06-01-1", "")
		if err != nil {
			t.Fatalf("неожиданная ошибка: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("resolveHours вернул %d часов, ожидался 1", len(got))
		}
	})

	t.Run("--backfill: конец не позже начала — ошибка", func(t *testing.T) {
		if _, err := resolveHours(true, "2026-06-02-0", "2026-06-01-0", ""); err == nil {
			t.Fatal("ожидалась ошибка при ДО <= ОТ")
		}
	})

	t.Run("--backfill: пустой диапазон (ОТ == ДО) — ошибка", func(t *testing.T) {
		if _, err := resolveHours(true, "2026-06-01-0", "2026-06-01-0", ""); err == nil {
			t.Fatal("ожидалась ошибка при ОТ == ДО (пустой диапазон)")
		}
	})
}

// pacedFetcher — тестовый fetcher (см. интерфейс fetcher в main.go) с точным управлением моментом,
// в который каждое событие считается «доставленным» в канал between fetch и produce. В отличие от
// httptest.Server поверх настоящего HTTP/gzip, здесь синхронизация идёт исключительно через
// небуферизованные каналы — тест детерминированно ловит момент «ровно N событий прочитано», без
// sleep и без гонки со временем.
type pacedFetcher struct {
	events []model.Event
	// sent — сигнал «одно событие успешно отправлено в out» после каждой отправки. Небуферизован
	// специально: FetchHour блокируется на нём, пока тест не вычитает сигнал, — это и есть
	// управление темпом извне.
	sent chan struct{}
}

func (f *pacedFetcher) FetchHour(ctx context.Context, _ time.Time, out chan<- model.Event) error {
	for _, evt := range f.events {
		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case f.sent <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// startTestRedpanda поднимает настоящий брокер Redpanda в Docker и создаёт в нём топик gh.events
// с шестью партициями (как redpanda-init в реальном окружении, ADR 0008) — тот же приём, что и в
// internal/producer/producer_test.go: проект не мокает датасторы, и для теста graceful shutdown
// критично, что Flush после отмены ctx реально доставляет данные, а не просто не падает.
func startTestRedpanda(t *testing.T) string {
	t.Helper()

	ctx := t.Context()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.4")
	if err != nil {
		t.Fatalf("запустить контейнер redpanda: %v", err)
	}
	t.Cleanup(func() {
		// НЕ t.Context() — он отменяется до вызова Cleanup-функций (таков контракт t.Context()),
		// а Terminate обязан реально дойти до Docker и остановить контейнер, а не оборваться сразу.
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("остановить контейнер redpanda: %v", err)
		}
	})

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("получить адрес брокера: %v", err)
	}

	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("собрать admin-клиент: %v", err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 6, 1, nil, "gh.events"); err != nil {
		t.Fatalf("создать топик gh.events: %v", err)
	}

	return broker
}

// overflowFetcher — тестовый fetcher для TestOrchestrateDispatchDoesNotDeadlockWhenHoursExceedWorkers.
// В отличие от pacedFetcher, здесь намеренно нет никакой синхронизации с тестом (нет "sent"-сигнала):
// цель не контролировать темп извне, а как можно быстрее отправить в out больше eventsPerHour
// событий, чем вмещает eventQueueSize, — воспроизводя условие настоящего часа GH Archive (100k+
// событий против буфера в 1000), при котором дедлок из orchestrate (см. её доккомментарий про
// errgroup.Go) фактически проявляется. EventID собран из часа и порядкового номера события, чтобы
// часы не пересекались по event_id и тест мог проверить, что событие каждого часа реально долетело.
type overflowFetcher struct {
	eventsPerHour int
}

func (f *overflowFetcher) FetchHour(ctx context.Context, hour time.Time, out chan<- model.Event) error {
	base := uint64(hour.Unix()) * 1_000_000
	for i := range f.eventsPerHour {
		evt := model.Event{
			EventID:   base + uint64(i),
			EventType: "WatchEvent",
			CreatedAt: hour.Add(time.Duration(i) * time.Millisecond),
			RepoID:    1,
			RepoName:  "a/b",
		}
		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// TestOrchestrateDispatchDoesNotDeadlockWhenHoursExceedWorkers — регрессионный тест на дедлок
// диспетчеризации errgroup.Go, описанный в доккомментарии orchestrate.
//
// errgroup.Go (golang.org/x/sync/errgroup v0.13.0, версия зафиксирована в go.mod) при исчерпанном
// лимите SetLimit блокируется СИНХРОННО в вызывающей горутине на семафоре, пока какая-нибудь уже
// запущенная задача не освободит слот, вернувшись из f(). Раньше цикл диспетчеризации
// ("for _, hour := range cfg.hours { fetchGroup.Go(...) }") шёл в той же горутине, что и последующий
// drain-цикл ("for evt := range events"), а не конкурентно с ним. При len(cfg.hours) > cfg.workers
// это вело к тотальному дедлоку: fetch-воркеры в полёте блокировались на отправке в переполненный
// events (буфер eventQueueSize=1000 меньше одного часа), читателя ещё не было, ни один из них не мог
// освободить слот в семафоре, а попытка запустить следующую по счёту часовую задачу зависала
// навечно. TestOrchestrateGracefulShutdownFlushesReadEvents эту ветку не ловил: там hours=1,
// workers=1 — ровно тот единственный случай, где дедлока в принципе быть не может (нет
// (workers+1)-й задачи, которую нужно было бы диспетчеризовать).
//
// Сценарий здесь — hours=3, workers=1, каждый час отдаёт заведомо больше eventQueueSize событий: до
// фикса дедлок ловится по таймауту select ниже (тест падает по t.Fatal, а не зависает вечно — сам
// пакет testing прервёт процесс своим собственным таймаутом, только когда автор забудет и это, но
// внутренний select здесь даёт куда более информативное сообщение об ошибке раньше). После фикса
// orchestrate обязана реально завершиться и доставить в Kafka все события всех трёх часов.
func TestOrchestrateDispatchDoesNotDeadlockWhenHoursExceedWorkers(t *testing.T) {
	broker := startTestRedpanda(t)

	// Заведомо больше eventQueueSize (1000): без этого запаса даже дедлочащая версия кода прошла бы
	// тест — при workers=1 диспетчеризация в буггованном коде фактически последовательна, и пока
	// один час помещается в буфер канала целиком, FetchHour успевает вернуться и освободить
	// семафор до того, как понадобится читатель. Дедлок проявляется только когда одиночный
	// fetch-воркер физически не может дописать все свои события в канал без параллельного читателя.
	const eventsPerHour = eventQueueSize + 500

	hours := []time.Time{
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC),
	}

	prodMetrics := producer.NewMetrics(prometheus.NewRegistry())
	prod, err := producer.New(t.Context(), producer.Config{Brokers: []string{broker}, Topic: "gh.events"}, prodMetrics, nil)
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	defer prod.Close()

	f := &overflowFetcher{eventsPerHour: eventsPerHour}

	orchestrateDone := make(chan error, 1)
	go func() {
		orchestrateDone <- orchestrate(t.Context(), f, prod, pipelineConfig{
			hours:           hours,
			workers:         1,
			sampleN:         0,
			shutdownTimeout: 30 * time.Second,
		}, io.Discard, io.Discard)
	}()

	select {
	case err := <-orchestrateDone:
		if err != nil {
			t.Fatalf("orchestrate вернула ошибку: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("orchestrate не завершилась за 60с — похоже на дедлок диспетчеризации errgroup.Go " +
			"при len(hours) > workers (см. доккомментарий orchestrate про перенос диспетчера в " +
			"отдельную горутину)")
	}

	// Проверяем содержимое топика напрямую (как и в TestOrchestrateGracefulShutdownFlushesReadEvents):
	// нам нужно убедиться, что дошли события ВСЕХ трёх часов, а не только первого, который до фикса
	// как раз и был единственным, что успевал стартовать.
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("gh.events"))
	if err != nil {
		t.Fatalf("собрать consumer-клиент: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancelFetch := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelFetch()

	want := len(hours) * eventsPerHour
	got := map[uint64]bool{}
	for len(got) < want {
		fetches := consumer.PollFetches(fetchCtx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll fetches: %v (доставлено %d/%d)", err, len(got), want)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var evt model.Event
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				t.Fatalf("Unmarshal записи из Kafka: %v", err)
			}
			got[evt.EventID] = true
		})
	}
	if len(got) != want {
		t.Fatalf("в Kafka долетело %d событий, ожидалось ровно %d (по одному разу на event_id всех трёх часов)",
			len(got), want)
	}
}

// TestRunModeValidation проверяет, что run() требует ровно один режим из трёх (--hour, --backfill,
// --live) и падает с ошибкой, оборачивающей errUsage, на любой другой комбинации — до того, как
// код успевает тронуть сеть/Kafka (иначе тест понадобился бы с Docker, как остальные в этом файле).
func TestRunModeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
	}{
		{name: "ни одного режима", args: []string{}},
		{name: "--hour и --live одновременно", args: []string{"--hour", "2026-06-01-15", "--live"}},
		{name: "--backfill и --live одновременно",
			args: []string{"--backfill", "2026-06-01-0", "2026-06-02-0", "--live"}},
		{name: "--hour и --backfill одновременно",
			args: []string{"--hour", "2026-06-01-15", "--backfill", "2026-06-01-0", "2026-06-02-0"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := run(tt.args)
			if !errors.Is(err, errUsage) {
				t.Fatalf("run(%v) = %v, want ошибку, оборачивающую errUsage", tt.args, err)
			}
		})
	}
}

// pacedRunner — тестовый runner (см. интерфейс runner в main.go) для runLive: тот же приём точного
// управления моментом "событие доставлено в канал", что и у pacedFetcher выше (см. её
// доккомментарий), только под сигнатуру Run(ctx, out) вместо FetchHour(ctx, hour, out).
//
// В отличие от pacedFetcher, где час GH Archive конечен и FetchHour возвращается сама, live-поллинг
// не заканчивается никогда, пока жив ctx, — после того как events исчерпан, pacedRunner блокируется
// на ctx.Done(), как и настоящий events.Client.Run в своём бесконечном цикле.
type pacedRunner struct {
	events []model.Event
	sent   chan struct{}
}

func (r *pacedRunner) Run(ctx context.Context, out chan<- model.Event) error {
	for _, evt := range r.events {
		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case r.sent <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestRunLiveGracefulShutdownFlushesReadEvents — тот же критерий приёмки, что и у
// TestOrchestrateGracefulShutdownFlushesReadEvents, только для live-режима: SIGINT (здесь — отмена
// ctx программно, см. её доккомментарий) останавливает поллинг и завершает процесс кодом 0, без
// потери уже полученных событий (флаш), но и не поставляя те, что live-источник ещё не успел отдать.
func TestRunLiveGracefulShutdownFlushesReadEvents(t *testing.T) {
	broker := startTestRedpanda(t)

	const totalEvents = 50
	const readBeforeCancel = 10

	liveEvents := make([]model.Event, totalEvents)
	for i := range liveEvents {
		liveEvents[i] = model.Event{
			EventID:   uint64(i + 1),
			EventType: "WatchEvent",
			CreatedAt: time.Date(2026, 7, 19, 10, 0, i, 0, time.UTC),
			RepoID:    1,
			RepoName:  "a/b",
		}
	}

	prodMetrics := producer.NewMetrics(prometheus.NewRegistry())
	prod, err := producer.New(t.Context(), producer.Config{Brokers: []string{broker}, Topic: "gh.events"}, prodMetrics, nil)
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	defer prod.Close()

	r := &pacedRunner{events: liveEvents, sent: make(chan struct{})}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runLiveDone := make(chan error, 1)
	go func() {
		runLiveDone <- runLive(ctx, r, prod, 30*time.Second, io.Discard, io.Discard)
	}()

	for i := range readBeforeCancel {
		select {
		case <-r.sent:
		case <-time.After(10 * time.Second):
			t.Fatalf("pacedRunner не отправил %d-е событие за 10с — тест завис", i+1)
		}
	}
	cancel() // имитация SIGINT

	select {
	case err := <-runLiveDone:
		if err != nil {
			t.Fatalf("runLive вернул ошибку при штатной остановке по SIGINT: %v (ожидался nil → exit code 0)", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("runLive не завершился после отмены ctx за 30с")
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("gh.events"))
	if err != nil {
		t.Fatalf("собрать consumer-клиент: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancelFetch := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancelFetch()

	got := map[uint64]bool{}
	for len(got) < readBeforeCancel {
		fetches := consumer.PollFetches(fetchCtx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll fetches: %v (доставлено %d/%d)", err, len(got), readBeforeCancel)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			var evt model.Event
			if err := json.Unmarshal(rec.Value, &evt); err != nil {
				t.Fatalf("Unmarshal записи из Kafka: %v", err)
			}
			got[evt.EventID] = true
		})
	}
	if len(got) < readBeforeCancel {
		t.Fatalf("в Kafka долетело %d событий, ожидалось хотя бы %d уже полученных до SIGINT", len(got), readBeforeCancel)
	}
	if got[totalEvents] {
		t.Errorf("событие event_id=%d долетело до Kafka — pacedRunner не должен был отдать его до отмены ctx", totalEvents)
	}
}

// TestOrchestrateGracefulShutdownFlushesReadEvents — сквозной тест на самый труднопроверяемый
// критерий приёмки задачи 1.4: «SIGINT во время бэкфилла завершает процесс с кодом 0, без потери
// уже прочитанных событий (флаш)». Реального SIGINT здесь нет (main_test.go не порождает
// подпроцесс) — вместо него ctx отменяется программно, что для orchestrate неотличимо от
// сигнала: run() строит ровно тот же ctx через signal.NotifyContext и передаёт его дальше без
// дополнительной логики (см. main.go).
//
// Сценарий: pacedFetcher отдаёт 50 событий одного часа, тест вычитывает ровно 10 сигналов о
// доставке в канал, затем отменяет ctx — имитация SIGINT ровно в момент, когда прочитана только
// часть часа. После этого orchestrate обязана: (1) вернуть nil (это и есть "код 0" — см. main(),
// который транслирует ошибку orchestrate в os.Exit только на non-nil), (2) доставить в Kafka
// все 10 уже прочитанных событий, (3) НЕ доставить событие, до которого чтение не дошло — иначе
// тест не отличал бы "остановились рано" от "случайно успели всё до отмены".
func TestOrchestrateGracefulShutdownFlushesReadEvents(t *testing.T) {
	broker := startTestRedpanda(t)

	const totalEvents = 50
	const readBeforeCancel = 10

	events := make([]model.Event, totalEvents)
	for i := range events {
		events[i] = model.Event{
			EventID:   uint64(i + 1),
			EventType: "WatchEvent",
			CreatedAt: time.Date(2026, 6, 1, 15, 0, i, 0, time.UTC),
			RepoID:    1,
			RepoName:  "a/b",
		}
	}

	prodMetrics := producer.NewMetrics(prometheus.NewRegistry())
	prod, err := producer.New(t.Context(), producer.Config{Brokers: []string{broker}, Topic: "gh.events"}, prodMetrics, nil)
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	defer prod.Close()

	f := &pacedFetcher{events: events, sent: make(chan struct{})}

	// context.WithCancel, а не просто t.Context(): здесь нужен ручной cancel() посреди теста —
	// имитация SIGINT в конкретный момент (после readBeforeCancel событий), а не отмена по
	// завершении теста, которую и так даёт t.Context() как страховку.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	orchestrateDone := make(chan error, 1)
	go func() {
		orchestrateDone <- orchestrate(ctx, f, prod, pipelineConfig{
			hours:           []time.Time{time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)},
			workers:         1,
			sampleN:         0,
			shutdownTimeout: 30 * time.Second,
		}, io.Discard, io.Discard)
	}()

	for i := range readBeforeCancel {
		select {
		case <-f.sent:
		case <-time.After(10 * time.Second):
			t.Fatalf("pacedFetcher не отправил %d-е событие за 10с — тест завис", i+1)
		}
	}
	cancel() // имитация SIGINT: код читал события, теперь останавливаем докачку

	select {
	case err := <-orchestrateDone:
		if err != nil {
			t.Fatalf("orchestrate вернула ошибку при штатной остановке по SIGINT: %v (ожидался nil → exit code 0)", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("orchestrate не завершилась после отмены ctx за 30с")
	}

	// Проверяем содержимое топика напрямую, а не полагаемся на отсутствие ошибки: Flush мог бы
	// вернуть nil и по-тихому ничего не доставить, если бы ctx.WithoutCancel был забыт в
	// Producer.Produce/Flush, — тест обязан увидеть настоящие события в Kafka, а не поверить коду.
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("gh.events"))
	if err != nil {
		t.Fatalf("собрать consumer-клиент: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancelFetch := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancelFetch()

	got := map[uint64]bool{}
	for len(got) < readBeforeCancel {
		fetches := consumer.PollFetches(fetchCtx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll fetches: %v (доставлено %d/%d)", err, len(got), readBeforeCancel)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var evt model.Event
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				t.Fatalf("Unmarshal записи из Kafka: %v", err)
			}
			got[evt.EventID] = true
		})
	}
	if len(got) < readBeforeCancel {
		t.Fatalf("в Kafka долетело %d событий, ожидалось хотя бы %d уже прочитанных до SIGINT", len(got), readBeforeCancel)
	}

	// Событие с конца списка (до него чтение точно не должно было дойти за 10 из 50 сигналов)
	// не должно было попасть в Kafka — иначе тест не отличал бы "остановились рано" от "случайно
	// прочитали всё до отмены".
	if got[totalEvents] {
		t.Errorf("событие event_id=%d долетело до Kafka — pacedFetcher не должен был отдать его до отмены ctx", totalEvents)
	}
}
