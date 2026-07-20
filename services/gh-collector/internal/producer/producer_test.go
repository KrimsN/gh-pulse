package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

// TestStickyKeyPartitionerIsDeterministic проверяет партиционер, зафиксированный в New
// (kgo.StickyKeyPartitioner(nil)), без похода в сеть и без брокера: TopicPartitioner — чистая
// функция от ключа и числа партиций, её можно вызвать напрямую. Это ровно то, что задача 1.4
// требует явно: «партиционер выбрать явно, а не унаследовать от библиотеки» (ADR 0008,
// «Последствия») — тест ловит регрессию, если кто-то в будущем уберёт kgo.RecordPartitioner(...)
// из New и партиционер молча откатится на умолчание библиотеки.
func TestStickyKeyPartitionerIsDeterministic(t *testing.T) {
	t.Parallel()

	const partitions = 6 // как у gh.events, см. ADR 0008
	partitioner := kgo.StickyKeyPartitioner(nil).ForTopic("gh.events")

	// Один и тот же ключ обязан детерминированно давать одну и ту же партицию — иначе повторный
	// прогон одного часа GH Archive раскладывал бы события иначе при каждом запуске, а ADR 0008
	// прямо называет детерминированность причиной, по которой отвергнут round-robin/без ключа
	// («раскладка стала бы функцией тайминга батчей клиента... нельзя ни воспроизвести, ни
	// измерить»).
	key := []byte("12660541189") // реальный event_id из internal/archive/testdata/sample_hour.jsonl
	rec := &kgo.Record{Key: key}
	first := partitioner.Partition(rec, partitions)
	for range 100 {
		if got := partitioner.Partition(rec, partitions); got != first {
			t.Fatalf("партиционер вернул разные партиции для одного и того же ключа: %d и %d", first, got)
		}
	}

	// Негативный контроль по образцу ADR 0008 (там же — «константа (контроль)» в таблице
	// перекоса): 1000 разных ключей обязаны разойтись хотя бы по нескольким партициям. Без этого
	// проверки выше «партиция стабильна» прошла бы и для сломанного партиционера, всегда
	// возвращающего 0, — тест не отличал бы «хэширует» от «не хэширует вовсе».
	seen := map[int]bool{}
	for i := range 1000 {
		p := partitioner.Partition(&kgo.Record{Key: []byte(strconv.Itoa(i))}, partitions)
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Fatalf("1000 разных ключей легли в %d партицию — партиционер не хэширует ключ", len(seen))
	}
}

// startRedpanda поднимает настоящий брокер Redpanda в Docker для интеграционных тестов —
// проект намеренно не мокает датасторы (см. CLAUDE.md), а Kafka-совместимый протокол плохо
// поддаётся честному мокированию поверх net.Listener без переизобретения половины клиента.
// Версия образа держится синхронно с infra/docker-compose.yml (redpandadata/redpanda:v24.2.4) —
// той же версией снято измерение перекоса в ADR 0008 (rpk v24.2.4).
func startRedpanda(t *testing.T) string {
	t.Helper()

	ctx := t.Context()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.4")
	if err != nil {
		t.Fatalf("запустить контейнер redpanda: %v", err)
	}
	t.Cleanup(func() {
		// НЕ t.Context() — он уже отменён к моменту вызова Cleanup-функций, а контейнер обязан
		// реально успеть остановиться, а не оборваться на середине запроса к Docker.
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("остановить контейнер redpanda: %v", err)
		}
	})

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("получить адрес брокера: %v", err)
	}
	return broker
}

// TestNewFailsWhenTopicMissing — критерий приёмки задачи 1.4 и прямое следствие ADR 0008:
// продюсер обязан упасть с понятной ошибкой, если топика нет, а не создать его молча (брокер
// в этом тестовом контейнере поднят без WithAutoCreateTopics — так же, как реальный продюсер не
// должен полагаться на auto_create_topics_enabled брокера, см. комментарий к ensureTopicExists).
func TestNewFailsWhenTopicMissing(t *testing.T) {
	ctx := t.Context()
	broker := startRedpanda(t)
	const topic = "gh.events.missing"

	_, err := New(ctx, Config{Brokers: []string{broker}, Topic: topic}, nil, nil)
	if !errors.Is(err, ErrTopicMissing) {
		t.Fatalf("New() вернул %v, ожидалась ошибка, оборачивающая ErrTopicMissing", err)
	}

	// New не просто должен вернуть ошибку — он не должен был попутно создать топик как побочный
	// эффект проверки (например, если бы кто-то по ошибке включил AllowAutoTopicCreation).
	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("собрать admin-клиент: %v", err)
	}
	defer admin.Close()

	details, err := kadm.NewClient(admin).ListTopics(ctx, topic)
	if err != nil {
		t.Fatalf("проверить список топиков: %v", err)
	}
	if detail, ok := details[topic]; ok && detail.Err == nil {
		t.Fatalf("топик %q существует после New() — продюсер не должен создавать топик сам", topic)
	}
}

// TestProduceDeliversWithEventIDKey гоняет полный путь Produce → Flush → реальная доставка в
// Kafka и проверяет два критерия приёмки задачи 1.4 разом: «события реально попадают в топик
// gh.events» и «ключ сообщения = event_id» (ADR 0008). Топик создаётся через kadm с шестью
// партициями — теми же параметрами, что и redpanda-init в infra/redpanda/create-topics.sh,
// только напрямую, без шелл-скрипта, ради простоты теста.
func TestProduceDeliversWithEventIDKey(t *testing.T) {
	ctx := t.Context()
	broker := startRedpanda(t)
	const topic = "gh.events"

	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("собрать admin-клиент: %v", err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 6, 1, nil, topic); err != nil {
		t.Fatalf("создать топик: %v", err)
	}

	prod, err := New(ctx, Config{Brokers: []string{broker}, Topic: topic}, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer prod.Close()

	want := []model.Event{
		{
			EventID: 1, EventType: "WatchEvent",
			CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			RepoID:    10, RepoName: "a/b",
		},
		{
			EventID: 2, EventType: "PushEvent",
			CreatedAt: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
			RepoID:    11, RepoName: "c/d", Ref: "refs/heads/main",
		},
	}
	for _, evt := range want {
		prod.Produce(ctx, evt)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("собрать consumer-клиент: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	gotEvents := map[uint64]model.Event{}
	gotKeys := map[uint64]string{}
	for len(gotEvents) < len(want) {
		fetches := consumer.PollFetches(fetchCtx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll fetches: %v", err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var evt model.Event
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				t.Fatalf("Unmarshal записи из Kafka: %v", err)
			}
			gotEvents[evt.EventID] = evt
			gotKeys[evt.EventID] = string(r.Key)
		})
	}

	for _, wantEvt := range want {
		gotEvt, ok := gotEvents[wantEvt.EventID]
		if !ok {
			t.Fatalf("событие event_id=%d не долетело до топика %s", wantEvt.EventID, topic)
		}
		if !gotEvt.CreatedAt.Equal(wantEvt.CreatedAt) {
			t.Errorf("event_id=%d: CreatedAt = %v, want %v", wantEvt.EventID, gotEvt.CreatedAt, wantEvt.CreatedAt)
		}
		gotCmp, wantCmp := gotEvt, wantEvt
		gotCmp.CreatedAt, wantCmp.CreatedAt = time.Time{}, time.Time{}
		if gotCmp != wantCmp {
			t.Errorf("событие после round-trip через Kafka:\n got  %+v\n want %+v", gotCmp, wantCmp)
		}

		wantKey := strconv.FormatUint(wantEvt.EventID, 10)
		if gotKeys[wantEvt.EventID] != wantKey {
			t.Errorf("ключ сообщения для event_id=%d = %q, want %q (ADR 0008: ключ — event_id)",
				wantEvt.EventID, gotKeys[wantEvt.EventID], wantKey)
		}
	}
}

// TestProduceObservesFailureOnOversizedRecord — задача 2.12: observeFailure (и NewMetrics) не были
// покрыты ни одним тестом, а единственный реалистичный способ увидеть настоящую ошибку доставки в
// Produce — запись, которую сам клиент (franz-go, ProducerBatchMaxBytes по умолчанию ~1 МиБ)
// отвергает как слишком большую ещё до похода в сеть. Не мок Kafka: тот же реальный клиент и
// брокер, что и в TestProduceDeliversWithEventIDKey, размер записи просто выбран заведомо больше
// лимита батча, чтобы сбой был детерминированным, а не зависел от занятости брокера.
func TestProduceObservesFailureOnOversizedRecord(t *testing.T) {
	ctx := t.Context()
	broker := startRedpanda(t)
	const topic = "gh.events"

	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("собрать admin-клиент: %v", err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 6, 1, nil, topic); err != nil {
		t.Fatalf("создать топик: %v", err)
	}

	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)

	prod, err := New(ctx, Config{Brokers: []string{broker}, Topic: topic}, metrics, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer prod.Close()

	oversized := model.Event{
		EventID:   999,
		EventType: "WatchEvent",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		RepoID:    1,
		RepoName:  strings.Repeat("x", 2*1024*1024), // заведомо больше лимита батча franz-go (~1 МиБ)
	}
	prod.Produce(ctx, oversized)
	if err := prod.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := testutil.ToFloat64(metrics.ProduceErrors); got != 1 {
		t.Fatalf("ProduceErrors = %v, want 1 — оверсайз-запись обязана была провалить доставку", got)
	}
	if got := testutil.ToFloat64(metrics.EventsProduced); got != 0 {
		t.Fatalf("EventsProduced = %v, want 0 — оверсайз-запись не должна засчитаться успехом", got)
	}
}
