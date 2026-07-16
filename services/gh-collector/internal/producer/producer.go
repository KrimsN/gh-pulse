// Package producer оборачивает Kafka-продюсер franz-go для доставки нормализованных событий
// GitHub в топик gh.events. Форма топика (ключ сообщения, число партиций, retention, кто его
// создаёт) спроектирована и обоснована в docs/adr/0008-gh-events-topic-design.md — этот пакет
// реализует три прямых следствия того ADR: явный партиционер (не умолчание библиотеки), отказ
// создавать топик самому и отказ проставлять метку времени сообщения руками.
package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

// ErrTopicMissing — сигнальная ошибка: топик, в который должен писать продюсер, не существует.
// New оборачивает её через %w, чтобы вызывающий код (cmd/gh-collector) мог отличить эту причину
// отказа от прочих (недоступный брокер, неверные флаги) через errors.Is, не разбирая текст.
var ErrTopicMissing = errors.New("kafka: topic does not exist")

// Metrics — Prometheus-метрики продюсера. Отдельная структура, а не пакетные счётчики — по той же
// причине, что и в internal/archive.Metrics: явная инъекция вместо общего DefaultRegisterer,
// чтобы тесты и несколько независимых Producer в одном процессе не конфликтовали за регистрацию.
type Metrics struct {
	EventsProduced prometheus.Counter
	ProduceErrors  prometheus.Counter
	ProduceLatency prometheus.Histogram
}

// NewMetrics создаёт и регистрирует метрики продюсера в reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_events_produced_total",
			Help: "Число событий, для которых Kafka подтвердила запись в gh.events. Скорость " +
				"роста этого счётчика (rate()) и есть events/sec из критериев приёмки задачи 1.4.",
		}),
		ProduceErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_produce_errors_total",
			Help: "Число событий, которые продюсер не смог доставить в Kafka.",
		}),
		ProduceLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gh_collector_produce_latency_seconds",
			Help:    "Время от вызова Produce до вызова промиса (успешного или нет) для одного события.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.EventsProduced, m.ProduceErrors, m.ProduceLatency)
	return m
}

// observeSuccess/observeFailure — nil-safe наблюдения (см. комментарий к тому же приёму в
// internal/archive.Metrics): тестам, которым метрики не важны, не нужно поднимать Registry.
func (m *Metrics) observeSuccess(started time.Time) {
	if m == nil {
		return
	}
	m.EventsProduced.Inc()
	m.ProduceLatency.Observe(time.Since(started).Seconds())
}

func (m *Metrics) observeFailure(started time.Time) {
	if m == nil {
		return
	}
	m.ProduceErrors.Inc()
	m.ProduceLatency.Observe(time.Since(started).Seconds())
}

// Config — параметры подключения продюсера.
type Config struct {
	Brokers []string // например []string{"localhost:9092"} — KAFKA_BROKERS из «Сквозных соглашений»
	Topic   string   // KAFKA_TOPIC, дефолт "gh.events"
}

// Producer — тонкая обёртка над *kgo.Client: фиксирует партиционер и топик, добавляет метрики и
// явную проверку существования топика при создании.
type Producer struct {
	client  *kgo.Client
	topic   string
	logger  *log.Logger
	metrics *Metrics
}

// New строит Producer и проверяет, что cfg.Topic уже существует на брокере. Возвращает
// ErrTopicMissing (через errors.Is), если это не так, — продюсер НЕ создаёт топик сам.
//
// Почему это вообще нужно проверять отдельно, а не просто попробовать писать и посмотреть на
// ошибку: у брокера (Redpanda в infra/docker-compose.yml) включено auto_create_topics_enabled=true
// по умолчанию, и топики создаёт одноразовый redpanda-init при docker compose up
// (infra/redpanda/create-topics.sh, обоснование — ADR 0008). Продюсер, стартовавший раньше
// инициализации или с опечаткой в имени топика, иначе получил бы новый топик с одной партицией
// вместо шести — пайплайн заработал бы, а параллельность консьюмера тихо схлопнулась. Явная
// проверка через admin API (kadm.ListTopics) с AllowAutoTopicCreation=false (её не выставляем
// нигде — это zero value kmsg.MetadataRequest) не полагается на то, что произойдёт "как получится"
// при первой записи, а падает с понятным сообщением до неё.
func New(ctx context.Context, cfg Config, metrics *Metrics, logger *log.Logger) (*Producer, error) {
	if cfg.Topic == "" {
		return nil, fmt.Errorf("producer: cfg.Topic is required")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("producer: cfg.Brokers is required")
	}
	if logger == nil {
		logger = log.Default()
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),

		// Партиционер выбран явно, а не унаследован от умолчания библиотеки (ADR 0008,
		// «Последствия»: «если Go-клиент возьмёт другую хэш-функцию, цифры для repo_id/actor_id
		// перестанут описывать систему»). Измерение равномерности в ADR сделано клиентом rpk —
		// murmur2-совместимый sticky-key, тот же алгоритм, что у Java-клиента по умолчанию до
		// Kafka 3.3 (KIP-480). Умолчание самого franz-go — другое и на момент написания этого
		// кода отличается от того, что измерялось: UniformBytesPartitioner (KIP-794, "uniform
		// sticky partitioner"), а не murmur2 sticky-key. Если бы здесь ничего не задали явно,
		// код молча уехал бы на другую партиционирующую функцию, и цифры перекоса из ADR 0008
		// перестали бы иметь отношение к реальному распределению этого продюсера.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),

		// Совпадает с compression.type=zstd топика (infra/redpanda/create-topics.sh). Без этой
		// опции клиент сжимал бы snappy (умолчание franz-go), и брокеру пришлось бы расжимать и
		// заново сжимать в zstd каждый батч, чтобы удовлетворить конфиг топика, — лишний проход
		// по каждому батчу на брокере без всякой пользы.
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),

		// Здесь намеренно нет kgo.AllowAutoTopicCreation() — опция выключена по умолчанию
		// (её отсутствие и есть выключение), и мы её не включаем по той же причине, по которой
		// ниже проверяем топик через kadm вместо того, чтобы полагаться на поведение брокера.
	)
	if err != nil {
		return nil, fmt.Errorf("producer: build kafka client: %w", err)
	}

	if err := ensureTopicExists(ctx, client, cfg.Topic); err != nil {
		client.Close()
		return nil, err
	}

	return &Producer{client: client, topic: cfg.Topic, logger: logger, metrics: metrics}, nil
}

// ensureTopicExists проверяет топик через Metadata-запрос (kadm.ListTopics), не создавая его.
func ensureTopicExists(ctx context.Context, client *kgo.Client, topic string) error {
	admin := kadm.NewClient(client)

	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("producer: list topics: %w", err)
	}

	detail, ok := details[topic]
	if !ok || detail.Err != nil {
		reason := "топик не найден"
		if detail.Err != nil {
			reason = detail.Err.Error()
		}
		return fmt.Errorf("%w: %q (%s) — топик создаёт redpanda-init при docker compose up "+
			"(ADR 0008); продюсер не создаёт его сам", ErrTopicMissing, topic, reason)
	}
	return nil
}

// Produce асинхронно отправляет одно событие в Kafka. Ключ сообщения — event_id (десятичной
// строкой): ADR 0008 выбирает его за структурную равномерность (уникален по определению), а не
// ради дедупликации — та решена на слое данных (ADR 0004) и ключа Kafka не требует.
//
// Асинхронность (kgo.Client.Produce с промисом, а не ProduceSync) — сознательный выбор
// пропускной способности важнее задержки одного события: если бы мы ждали ack брокера на каждое
// событие по очереди, продюсер работал бы на скорости одного round-trip'а до брокера за событие
// вместо того, чтобы батчить события внутри клиента.
//
// Backpressure получается тем же путём "бесплатно": если у клиента заполнен внутренний буфер
// (kgo.MaxBufferedRecords, дефолт 10000 записей), сам вызов Produce блокируется, пока не
// освободится место. Вызывающий код (worker pool в cmd/gh-collector) поэтому не нуждается в
// отдельном ограничении скорости — блокировка Produce уже тормозит консьюмера канала событий,
// который в свою очередь тормозит fetcher через bounded-канал между ними (internal/archive.
// FetchHour).
//
// ctx определяет только время ожидания места в буфере и, будучи привязан к записи, может позже
// оборвать её отправку при отмене (см. kgo.Client.Produce). Из-за этого cmd/gh-collector обязан
// передавать сюда контекст, НЕ отменяемый напрямую сигналом SIGINT, во время фазы флаша перед
// выходом — иначе уже прочитанные из GH Archive события просто отбрасывались бы вместо доставки,
// а не «флашились», что прямо нарушает критерий приёмки задачи 1.4.
func (p *Producer) Produce(ctx context.Context, evt model.Event) {
	started := time.Now()

	value, err := json.Marshal(evt)
	if err != nil {
		// model.Event — плоская структура из скалярных полей и time.Time; у обоих Marshal не
		// падает на валидных значениях. Если это всё же случилось — баг в модели, а не во входных
		// данных GitHub, и его лучше увидеть в метриках/логе, чем проглотить молча.
		p.logger.Printf("producer: marshal event_id=%d: %v", evt.EventID, err)
		p.metrics.observeFailure(started)
		return
	}

	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(strconv.FormatUint(evt.EventID, 10)),
		Value: value,
		// Timestamp намеренно не выставляется. Топик сконфигурирован с
		// message.timestamp.type=LogAppendTime (ADR 0008) — метку в любом случае перезапишет
		// брокер при записи в лог; свой Timestamp здесь означал бы код, который выглядит так,
		// будто на что-то влияет, а на самом деле нет. created_at самого события и так едет
		// внутри Value как обычное поле JSON.
	}

	p.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if err != nil {
			p.logger.Printf("producer: deliver event_id=%d to %s: %v", evt.EventID, p.topic, err)
			p.metrics.observeFailure(started)
			return
		}
		p.metrics.observeSuccess(started)
	})
}

// Flush ждёт подтверждения всех буферизованных на клиенте записей — успешного или нет, в любом
// случае их промисы будут вызваны до возврата.
//
// Вызывающий код обязан передавать ctx, НЕ отменённый вместе с сигналом SIGINT (см. Produce выше
// и cmd/gh-collector/main.go): если бы ctx.Done() сработал раньше, чем реально долетели последние
// батчи, Flush вернулся бы досрочно с ctx.Err(), и требование "флаш перед выходом" из критерия
// приёмки задачи 1.4 выполнялось бы только на бумаге, а не по факту.
func (p *Producer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("producer: flush: %w", err)
	}
	return nil
}

// Close освобождает соединения клиента. Close сам не ждёт доставки буферизованных записей — это
// делает только Flush, вызывать его нужно раньше.
func (p *Producer) Close() {
	p.client.Close()
}
