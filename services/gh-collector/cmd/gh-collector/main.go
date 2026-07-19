// cmd/gh-collector — точка входа коллектора. Умеет три режима: один час GH Archive (--hour),
// диапазон часов (--backfill ОТ ДО) или живой поллинг GitHub Events API (--live). Backfill-режимы
// скачивают часы worker pool'ом ограниченной ширины (--workers), live — единственным бесконечным
// циклом поллинга (internal/events); оба продюсят нормализованные события в Kafka
// (internal/producer) через тот же паттерн backpressure и graceful shutdown. Между fetch- и
// produce-стадиями — ограниченный канал: если Kafka не успевает, fetch (или поллинг) тормозит сам,
// вместо того чтобы копить события в памяти без предела. SIGINT/SIGTERM останавливают докачку
// новых часов (или новых поллов) и флашат уже прочитанные события перед выходом (graceful
// shutdown) — см. orchestrate и runLive ниже.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/archive"
	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/events"
	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/producer"
)

// errUsage — сигнальная ошибка неверных флагов/аргументов. main отличает её от сбоя fetch/produce
// через errors.Is, чтобы вернуть exit code 2 (конвенция CLI-утилит: 2 = неверное использование).
var errUsage = errors.New("usage")

const (
	// eventQueueSize — размер буфера канала между fetch-воркерами и продюсером. Буферизованный,
	// а не безграничный (`[]model.Event` со своим append), — это и есть backpressure задачи 1.4:
	// как только канал заполнен, отправка в него (внутри archive.Client.FetchHour) блокируется, и
	// fetch перестаёт качать данные из сети быстрее, чем их успевает принять продюсер. Число
	// не масштабируется от --workers: переполнение канала — это давление на consumer'а
	// (producer.Producer.Produce), а не на количество одновременных fetch-воркеров, так что
	// больше воркеров не требует большего буфера для того, чтобы backpressure сработал корректно.
	//
	// Неявная связь с franz-go, которую стоит иметь в виду при изменении любой из двух констант:
	// --shutdown-timeout (см. defaultShutdownTimeout ниже) по факту ограничивает только
	// producer.Producer.Flush в конце orchestrate, а не сам drain-цикл ("for evt := range events")
	// — prod.Produce там вызывается с produceCtx = context.WithoutCancel(ctx), который никогда не
	// станет Done. Сегодня это безопасно только потому, что eventQueueSize (1000) заведомо меньше
	// kgo.MaxBufferedRecords (дефолт 10000 в franz-go, см. доккомментарий Producer.Produce в
	// internal/producer/producer.go): канал events исчерпается и закроется раньше, чем успеет
	// заполниться внутренний буфер клиента Kafka. Если бы буфер клиента переполнился первым, вызов
	// Produce внутри drain-цикла заблокировался бы без какого-либо таймаута — drain-цикл завис бы
	// на неопределённое время уже после SIGINT, до всякого Flush. Если один из двух конкретных
	// чисел когда-нибудь изменится (заводить их за это в один флаг/переменную сейчас смысла нет —
	// это разные слои: bounded-канал между стадиями и внутренний буфер клиента), эту гарантию нужно
	// будет пересмотреть заново, а не считать её вечной.
	eventQueueSize = 1000

	defaultMetricsAddr     = ":9469"
	defaultShutdownTimeout = 30 * time.Second
	defaultSampleN         = 3
)

func main() {
	err := run(os.Args[1:])
	switch {
	case err == nil:
		return
	case errors.Is(err, errUsage):
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	default:
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run разбирает флаги, строит зависимости (archive.Client, producer.Producer, HTTP-сервер метрик)
// и делегирует всю сетевую работу orchestrate. Возвращает error вместо прямого os.Exit/log.Fatal —
// оба обрывают процесс немедленно и пропускают все defer в стеке вызовов, а здесь на defer держится
// и остановка HTTP-сервера метрик, и Producer.Close(); os.Exit внутри run было бы тихой поломкой
// graceful shutdown на любом пути завершения.
//
// args — без имени программы (os.Args[1:]), а не сам os.Args: это делает run вызываемым из тестов
// с произвольным набором аргументов без манипуляций с os.Args (глобальным для всего процесса).
func run(args []string) error {
	from, to, rest, isBackfill, err := extractBackfillRange(args)
	if err != nil {
		return err
	}

	// flag.NewFlagSet, а не пакетный flag.CommandLine: последний — процессный глобал, повторный
	// flag.String/flag.Parse в рамках одного процесса (например, из нескольких тестов, вызывающих
	// run() с разными args) паникует на "flag redefined". Свой FlagSet на каждый вызов run() от
	// этого не страдает и попутно делает run() тестируемым без спец-ухищрений.
	fs := flag.NewFlagSet("gh-collector", flag.ContinueOnError)
	hourFlag := fs.String("hour", "", "час GH Archive в UTC, формат YYYY-MM-DD-H, например 2026-06-01-15")
	workers := fs.Int("workers", runtime.NumCPU(), "сколько часов бэкфилла скачивать параллельно (worker pool fetch-стадии)")
	sampleN := fs.Int("sample", defaultSampleN, "сколько первых прочитанных событий распечатать как сэмпл")
	metricsAddr := fs.String("metrics-addr", defaultMetricsAddr, "адрес HTTP-сервера метрик Prometheus (эндпоинт /metrics)")
	shutdownTimeout := fs.Duration("shutdown-timeout", defaultShutdownTimeout,
		"сколько ждать доставки уже прочитанных событий в Kafka после SIGINT/SIGTERM, прежде чем считать остановку неуспешной")
	kafkaBrokers := fs.String("kafka-brokers", envOr("KAFKA_BROKERS", "localhost:9092"),
		"адреса брокеров Kafka через запятую (или переменная окружения KAFKA_BROKERS)")
	kafkaTopic := fs.String("kafka-topic", envOr("KAFKA_TOPIC", "gh.events"),
		"топик Kafka для событий (или переменная окружения KAFKA_TOPIC); должен существовать заранее, см. ADR 0008")
	liveFlag := fs.Bool("live", false,
		"живой поллинг GitHub Events API вместо GH Archive (нужен ровно один режим — --hour, --backfill или --live)")
	githubToken := fs.String("github-token", envOr("GITHUB_TOKEN", ""),
		"токен GitHub для --live (или переменная окружения GITHUB_TOKEN); без токена поллинг неаутентифицированный (60 запросов/час вместо 5000)")

	if err := fs.Parse(rest); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}

	// Ровно один режим из трёх: два юридических способа сказать "качать GH Archive" (--hour,
	// --backfill) плюс --live. isBackfill и *hourFlag!="" уже взаимоисключающие по построению
	// extractBackfillRange/fs.Parse (--backfill съедает свои позиционные аргументы до разбора
	// флагов), так что здесь достаточно посчитать, сколько из трёх условий истинно.
	modesSet := 0
	if isBackfill {
		modesSet++
	}
	if *hourFlag != "" {
		modesSet++
	}
	if *liveFlag {
		modesSet++
	}
	if modesSet != 1 {
		return fmt.Errorf("%w: нужен ровно один режим — --hour ЧАС, --backfill ОТ ДО или --live", errUsage)
	}
	if *sampleN < 0 {
		return fmt.Errorf("%w: --sample не может быть отрицательным, получено %d", errUsage, *sampleN)
	}
	if *workers < 1 {
		return fmt.Errorf("%w: --workers должен быть не меньше 1, получено %d", errUsage, *workers)
	}

	// hours нужен только не-live режимам — resolveHours() вызывается ниже условно, чтобы --live не
	// требовал ни --hour, ни --backfill заполненными (у него нет часов вовсе).
	var hours []time.Time
	if !*liveFlag {
		hours, err = resolveHours(isBackfill, from, to, *hourFlag)
		if err != nil {
			return fmt.Errorf("%w: %w", errUsage, err)
		}
	}

	// TrimSpace на каждый элемент и отбрасывание пустых — иначе "a, b" (обычный способ написать
	// список через запятую, с пробелом после разделителя) дал бы ["a", " b"] с ведущим пробелом в
	// адресе брокера, а --kafka-brokers "" дал бы [""] — непустой срез длины 1, который проходит
	// мимо проверки producer.New на len(cfg.Brokers)==0 и падает позже с менее понятной сетевой
	// ошибкой вместо явного errUsage здесь.
	//
	// strings.SplitSeq, а не strings.Split: результат разбора здесь только проходится циклом и
	// нигде не нужен как срез целиком (ни индексация, ни len до цикла) — SplitSeq не аллоцирует
	// промежуточный []string под весь список брокеров.
	var brokers []string
	for b := range strings.SplitSeq(*kafkaBrokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return fmt.Errorf("%w: --kafka-brokers должен содержать хотя бы один непустой адрес брокера", errUsage)
	}

	// Свой Registry, а не prometheus.DefaultRegisterer: последний — процессный глобал, и запуск
	// нескольких run() в одном процессе (тесты) паниковал бы на повторной регистрации одних и тех
	// же имён метрик.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	archiveMetrics := archive.NewMetrics(reg)
	producerMetrics := producer.NewMetrics(reg)
	// events.NewMetrics регистрирует настоящий Gauge gh_collector_github_rate_limit_remaining —
	// до этой задачи он был захардкожен нулём прямо здесь (заглушка "метрика ещё не подключена",
	// GH Archive рейт-лимитов не имеет). Регистрируем безусловно, а не только в ветке --live: в
	// backfill-режиме Client.Run никогда не запускается, и Gauge просто остаётся на нулевом
	// значении, тем же способом, каким и раньше сигнализировал "не задействован", — но теперь это
	// поведение настоящего, а не притворного Gauge.
	eventsMetrics := events.NewMetrics(reg)

	metricsServer := &http.Server{Addr: *metricsAddr, Handler: metricsHandler(reg)}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "metrics: %v\n", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	// SIGINT (Ctrl+C) и SIGTERM (docker stop / systemd) отменяют корневой context — это и запускает
	// graceful shutdown: orchestrate прекращает докачивать новые часы и переходит к флашу уже
	// прочитанных событий (см. её доккомментарий).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	prod, err := producer.New(ctx, producer.Config{Brokers: brokers, Topic: *kafkaTopic}, producerMetrics, nil)
	if err != nil {
		return fmt.Errorf("build producer: %w", err)
	}
	defer prod.Close()

	if *liveFlag {
		if *githubToken == "" {
			// Не hard requirement (критерий приёмки задачи 2.9 явно это оговаривает) — только
			// предупреждение: неаутентифицированный поллинг работает, просто на лимите 60/час
			// вместо 5000/час.
			fmt.Fprintln(os.Stderr, "gh-collector: GITHUB_TOKEN не задан — поллинг Events API "+
				"неаутентифицированный (лимит 60 запросов/час вместо 5000)")
		}
		eventsClient := events.NewClient(*githubToken, eventsMetrics, nil)
		return runLive(ctx, eventsClient, prod, *shutdownTimeout, os.Stdout, os.Stderr)
	}

	archiveClient := archive.NewClient(archiveMetrics, nil)
	return orchestrate(ctx, archiveClient, prod, pipelineConfig{
		hours:           hours,
		workers:         *workers,
		sampleN:         *sampleN,
		shutdownTimeout: *shutdownTimeout,
	}, os.Stdout, os.Stderr)
}

// metricsHandler строит HTTP-обработчик /metrics для Registry reg.
func metricsHandler(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// envOr читает переменную окружения key; если она не задана (в том числе пустой строкой),
// возвращает fallback. os.LookupEnv, а не os.Getenv: Getenv не отличает "переменная не задана" от
// "переменная задана пустой строкой" — оба случая дают "", а нам явно нужен только первый как
// повод подставить дефолт.
func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

// extractBackfillRange отделяет «--backfill ОТ ДО» от остальных флагов до вызова flag.Parse.
//
// Пакет flag из stdlib останавливает разбор на первом позиционном (не начинающемся с "-")
// аргументе — а форма вызова из примера CLI задачи 1.4, "--backfill 2026-06-01-0 2026-06-02-0
// --workers 8", как раз с этого и начинается: "2026-06-01-0" идёт сразу после --backfill и сам не
// флаг. Обычный flag.Parse после такого аргумента прекратил бы разбор и отдал бы "--workers 8" в
// остаток нетронутым текстом, а не как флаг. Через стороннюю CLI-библиотеку это решается штатно,
// но заводить зависимость ради одной формы вызова не стоит: значения ОТ/ДО — это час GH Archive
// вида YYYY-MM-DD-H, а такая строка никогда не начинается с "-", так что вырезать её из args
// вручную, до основного разбора, безопасно и однозначно — настоящий флаг с датой не спутать.
func extractBackfillRange(args []string) (from, to string, remainder []string, found bool, err error) {
	for i, a := range args {
		if a != "--backfill" && a != "-backfill" {
			continue
		}
		if i+2 >= len(args) {
			return "", "", nil, true, fmt.Errorf(
				"%w: --backfill требует двух аргументов ОТ и ДО сразу после себя, например --backfill 2026-06-01-0 2026-06-02-0",
				errUsage)
		}
		from, to = args[i+1], args[i+2]
		remainder = make([]string, 0, len(args)-3)
		remainder = append(remainder, args[:i]...)
		remainder = append(remainder, args[i+3:]...)
		return from, to, remainder, true, nil
	}
	return "", "", args, false, nil
}

// resolveHours превращает --hour или --backfill ОТ ДО в список часов для worker pool'а.
// Верхняя граница диапазона --backfill исключающая: hours покрывает [from, to), тем же способом,
// каким Go сам определяет диапазоны через range по срезу — час, равный to, в результат не входит.
// Пример из задачи 1.4 (--backfill 2026-06-01-0 2026-06-02-0) поэтому даёт ровно 24 часа одних
// суток, а не 25.
func resolveHours(isBackfill bool, from, to, hourFlag string) ([]time.Time, error) {
	if !isBackfill {
		h, err := parseHour(hourFlag)
		if err != nil {
			return nil, fmt.Errorf("разбор --hour: %w", err)
		}
		return []time.Time{h}, nil
	}

	fromT, err := parseHour(from)
	if err != nil {
		return nil, fmt.Errorf("разбор начала диапазона --backfill: %w", err)
	}
	toT, err := parseHour(to)
	if err != nil {
		return nil, fmt.Errorf("разбор конца диапазона --backfill: %w", err)
	}
	if !toT.After(fromT) {
		return nil, fmt.Errorf("--backfill: конец диапазона (%s) должен быть строго позже начала (%s)", to, from)
	}

	hours := make([]time.Time, 0, int(toT.Sub(fromT).Hours()))
	for h := fromT; h.Before(toT); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	return hours, nil
}

// pipelineConfig — параметры одного прогона orchestrate.
type pipelineConfig struct {
	hours           []time.Time
	workers         int
	sampleN         int
	shutdownTimeout time.Duration
}

// fetcher — минимальный интерфейс, который нужен orchestrate от источника часов. Определён здесь,
// в пакете-потребителе, а не в internal/archive, — идиома Go «принимай интерфейсы, возвращай
// структуры»: archive.Client ничего не знает про этот интерфейс и не обязан ему ничего
// реализовывать явно, он ему соответствует просто по сигнатуре метода (структурная типизация).
// Тестам это даёт возможность подставить fetcher с точным контролем момента доставки каждого
// события через каналы — без реального HTTP/gzip там, где важна именно синхронизация с отменой
// ctx, а не поход в сеть (см. pacedFetcher в main_test.go). Kafka-сторона (producer.Producer)
// такого интерфейса намеренно не получает: датастор в тестах поднимается настоящий (testcontainers),
// а не мокается — здесь абстракция нужна для другой оси (тайминг GH Archive), не для замены брокера.
type fetcher interface {
	FetchHour(ctx context.Context, hour time.Time, out chan<- model.Event) error
}

// orchestrate качает hours worker pool'ом шириной cfg.workers и продюсит события в Kafka.
// Вынесена из run(), чтобы тесты могли передать уже готовый producer.Producer (указывающий на
// testcontainers-брокер) и fetcher с управляемым таймингом, а также context, отменяемый
// программно, — signal.NotifyContext в run() слушает настоящие ОС-сигналы и не поддаётся отмене
// «понарошку» из теста.
//
// Graceful shutdown устроен в три стадии, и порядок здесь принципиален:
//
//  1. Пока ctx жив, errgroup качает часы worker pool'ом шириной cfg.workers, каждый воркер пишет
//     события в общий bounded-канал (backpressure — см. eventQueueSize).
//  2. Продюсер вычитывает канал до его закрытия НЕЗАВИСИМО от состояния ctx — если бы цикл сам
//     проверял ctx.Done() и выходил по отмене, уже прочитанные из GH Archive события, лежащие в
//     канале, остались бы недоставленными: это и есть потеря данных, которую критерий приёмки
//     задачи 1.4 явно запрещает ("без потери уже прочитанных событий").
//  3. После того как канал закрылся (все fetch-воркеры отработали или ушли по отмене ctx), ждём
//     Flush с отдельным, НЕ связанным с ctx таймаутом. Тот же принцип, что и в пункте 2, только на
//     уровне producer.Producer.Flush: SIGINT не должен прерывать уже поставленную в очередь
//     доставку, а лишь ограничивать её сверху по времени (--shutdown-timeout), чтобы процесс не
//     завис навечно, если Kafka не отвечает вовсе.
func orchestrate(ctx context.Context, archiveClient fetcher, prod *producer.Producer, cfg pipelineConfig, stdout, stderr io.Writer) error {
	if len(cfg.hours) == 0 {
		return fmt.Errorf("orchestrate: список часов пуст")
	}

	events := make(chan model.Event, eventQueueSize)

	// errgroup вместо ручного паттерна "горутина пишет fetchErr в замыкание, close(events),
	// потом читаем fetchErr" (так было в задаче 1.3): там корректность держалась на том, что
	// range по каналу всегда доходит до close без break. Здесь несколько часов и (потенциально)
	// первая ошибка любого из них должны прервать остальные — типичный "break" по ошибке, после
	// которого access к результату без errgroup стал бы гонкой, не всегда ловимой -race. errgroup
	// покрывает это по построению: WithContext даёт derived context, отменяемый по первой ошибке
	// любого воркера, а Wait() синхронизированно возвращает эту первую ошибку без ручных мьютексов.
	//
	// SetLimit(cfg.workers), а не cfg.workers отдельных горутин на весь список часов: Go
	// ограничивает часы бэкфилла в полёте одновременно, а не события внутри часа — ровно то, что
	// требует критерий приёмки "--workers меняет фактическую конкурентность" (распараллеливание по
	// часам, задача 1.4).
	fetchGroup, fetchCtx := errgroup.WithContext(ctx)
	fetchGroup.SetLimit(cfg.workers)

	// Диспетчеризация (fetchGroup.Go на каждый час), fetchGroup.Wait() и close(events) — всё в одной
	// отдельной горутине, а НЕ в теле orchestrate, как было раньше (и приводило к дедлоку на любом
	// реальном --backfill). Причина в самом errgroup: Go (см. исходник golang.org/x/sync/errgroup
	// v0.13.0, версия зафиксирована в go.mod) при исчерпанном лимите SetLimit блокируется СИНХРОННО
	// в вызывающей горутине на семафоре (`g.sem <- token{}`), пока какая-то из уже запущенных задач
	// не освободит слот, вернувшись из f(). Если бы цикл диспетчеризации оставался в теле orchestrate,
	// а drain-цикл ("for evt := range events" ниже) стартовал только после него, — все cfg.workers
	// горутин FetchHour зависли бы на отправке в events (eventQueueSize=1000 заведомо меньше одного
	// часа дампа — 100k+ событий, а читателя канала ещё нет), ни одна из них не смогла бы вернуться
	// из f() и освободить слот в семафоре, а (workers+1)-й вызов fetchGroup.Go заблокировался бы
	// навечно в ожидании этого освобождения — тотальный дедлок при len(cfg.hours) > cfg.workers.
	// Вынося диспетчер в отдельную горутину, мы даём drain-циклу в теле orchestrate стартовать сразу
	// и вычитывать events конкурентно с диспатчем и с работой воркеров — цикл ожидания разрывается.
	//
	// Продюсерский цикл ниже при этом по-прежнему просто делает "for range events" и не обязан
	// ничего знать про errgroup: канал закрывает тот, кто точно знает, что закончили все отправители
	// разом (fetchGroup.Wait()), — сама archive.Client.FetchHour закрывать его не может и не должна
	// (см. её доккомментарий).
	fetchDone := make(chan error, 1)
	go func() {
		for _, hour := range cfg.hours {
			// Захват hour по значению — не нужен отдельный hour := hour: начиная с Go 1.22 (этот
			// модуль — go 1.25 в go.mod) переменная цикла for создаётся заново на каждой итерации,
			// и замыкание ниже не может увидеть значение из чужой итерации, как было в более старых
			// версиях языка.
			fetchGroup.Go(func() error {
				return archiveClient.FetchHour(fetchCtx, hour, events)
			})
		}
		fetchDone <- fetchGroup.Wait()
		close(events)
	}()

	// produceCtx — НЕ ctx и не fetchCtx: context.WithoutCancel возвращает контекст с тем же деревом
	// значений, но который никогда не станет Done из-за отмены родителя. Дословно из
	// producer.Producer.Produce: если сюда передать отменяемый ctx, SIGINT прервёт доставку уже
	// прочитанных событий вместо того, чтобы дать им долететь, — то есть заставит "флаш" из
	// критерия приёмки на самом деле отбрасывать данные.
	produceCtx := context.WithoutCancel(ctx)

	read := 0
	samples := make([]model.Event, 0, cfg.sampleN)
	for evt := range events {
		read++
		if len(samples) < cfg.sampleN {
			samples = append(samples, evt)
		}
		prod.Produce(produceCtx, evt)
	}
	fetchErr := <-fetchDone

	// Ошибки записи в stdout/stderr здесь осознанно глушим: это диагностический вывод, а не
	// часть контракта команды — потерянная строка лога не повод провалить весь бэкфилл, который
	// к этому моменту уже фактически завершён (события либо доставлены, либо ушли по ошибке ниже).
	for _, s := range samples {
		_, _ = fmt.Fprintf(stdout, "sample: %+v\n", s)
	}
	_, _ = fmt.Fprintf(stdout, "hours=%d events_read=%d\n", len(cfg.hours), read)

	stopped := errors.Is(fetchErr, context.Canceled)
	if stopped {
		_, _ = fmt.Fprintf(stderr, "остановлено пользователем (SIGINT/SIGTERM): докачка новых часов прекращена, "+
			"флашим %d уже прочитанных событий в Kafka...\n", read)
	}

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancelFlush()
	if err := prod.Flush(flushCtx); err != nil {
		return fmt.Errorf("flush producer: %w", err)
	}

	switch {
	case stopped:
		// exit code 0, а не 130 (конвенция "128+SIGINT" из задачи 1.3): там 130 означало
		// "прервано посреди работы, без гарантий сохранности", а здесь Flush выше только что
		// подтвердил обратное — все прочитанные события доставлены. 130 для честного graceful
		// stop был бы неверным сигналом любому, кто автоматизирует запуск этой команды
		// (например, systemd/CI: ненулевой код выглядел бы как настоящий сбой).
		return nil
	case fetchErr != nil:
		return fmt.Errorf("backfill: %w", fetchErr)
	default:
		return nil
	}
}

// runner — минимальный интерфейс, который нужен runLive от источника live-событий. Определён здесь,
// в пакете-потребителе, той же идиомой, что и fetcher выше ("принимай интерфейсы, возвращай
// структуры"): events.Client ничего не знает про этот интерфейс, просто соответствует ему по
// сигнатуре метода. Тестам это даёт возможность подставить runner с точным контролем момента
// доставки и отмены, без реального HTTP — тот же приём, что и pacedFetcher в main_test.go.
type runner interface {
	Run(ctx context.Context, out chan<- model.Event) error
}

// runLive запускает бесконечный поллинг GitHub Events API и продюсит нормализованные события в
// Kafka. Устроена по тому же принципу graceful shutdown, что и orchestrate — поллинг прекращается
// по ctx, канал вычитывается до закрытия независимо от состояния ctx (иначе уже полученные события
// потерялись бы), Flush ждёт отдельным таймаутом, не связанным с ctx (см. подробное обоснование
// каждого из этих трёх пунктов в доккомментарии orchestrate — оно дословно применимо и здесь).
//
// Код не переиспользован с orchestrate буквально: у неё конечный список часов и errgroup-worker
// pool по ним, у runLive — единственный бесконечный источник (client.Run), и вводить общий для
// обоих интерфейс/обёртку ради структурного сходства значило бы городить абстракцию под сценарий,
// которого нет, — то, от чего явно предостерегает CLAUDE.md этого репозитория.
func runLive(ctx context.Context, client runner, prod *producer.Producer, shutdownTimeout time.Duration, stdout, stderr io.Writer) error {
	out := make(chan model.Event, eventQueueSize)

	runDone := make(chan error, 1)
	go func() {
		runDone <- client.Run(ctx, out)
		close(out)
	}()

	// produceCtx — то же обоснование, что и в orchestrate: SIGINT не должен прерывать доставку уже
	// полученных событий, только останавливать приём новых.
	produceCtx := context.WithoutCancel(ctx)

	read := 0
	for evt := range out {
		read++
		prod.Produce(produceCtx, evt)
	}
	runErr := <-runDone

	_, _ = fmt.Fprintf(stdout, "live: events_read=%d\n", read)

	stopped := errors.Is(runErr, context.Canceled)
	if stopped {
		_, _ = fmt.Fprintf(stderr, "остановлено пользователем (SIGINT/SIGTERM): поллинг прекращён, "+
			"флашим %d уже полученных событий в Kafka...\n", read)
	}

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelFlush()
	if err := prod.Flush(flushCtx); err != nil {
		return fmt.Errorf("flush producer: %w", err)
	}

	switch {
	case stopped:
		return nil // exit code 0 — то же обоснование, что и в orchestrate: Flush выше уже подтвердил доставку.
	case runErr != nil:
		return fmt.Errorf("live: %w", runErr)
	default:
		return nil
	}
}

// parseHour разбирает YYYY-MM-DD-H (час без ведущего нуля — как в именах файлов GH Archive).
//
// Дату проверяет time.Parse("2006-01-02", ...): в отличие от fmt.Sscanf с "%d-%d-%d-%d", он сам
// отвергает как хвост после даты, так и внедиапазонные месяц/день (эмпирически проверено: "2026-
// 13-45" -> "month out of range", "2026-02-30" -> "day out of range", "2026-06-01-garbage" ->
// "extra text"). Sscanf ничего из этого не делал: он молча брал первые совпавшие %d и игнорировал
// остаток строки, а time.Date после него молча нормализовал (не отвергал) любой выход за
// диапазон — "2026-06-01-24" превращался в 2026-06-02-0, "2026-00-00-0" — в 2025-11-30. Опечатка
// в CLI-флаге тихо скачивала другой час вместо ошибки — худший класс бага для этого проекта.
//
// Час разбирается отдельно strconv.Atoi с явной проверкой 0..23, потому что layout "15" в
// time.Parse требует двух цифр, а GH Archive пишет час без ведущего нуля ("...-9", не "...-09").
func parseHour(s string) (time.Time, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		return time.Time{}, fmt.Errorf("ожидается формат YYYY-MM-DD-H (4 поля через дефис), получено %q", s)
	}

	day, err := time.Parse("2006-01-02", strings.Join(parts[:3], "-"))
	if err != nil {
		return time.Time{}, fmt.Errorf("разбор даты в %q: %w", s, err)
	}

	hour, err := strconv.Atoi(parts[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("час должен быть числом, получено %q", parts[3])
	}
	// Только верхняя граница: parts[3] получен из strings.Split(s, "-") и физически не может
	// начинаться с "-" (минус сам — разделитель, на котором строка уже разрезана), поэтому
	// strconv.Atoi(parts[3]) выше никогда не вернёт отрицательное число — проверка hour < 0 была бы
	// недостижимой веткой.
	if hour > 23 {
		return time.Time{}, fmt.Errorf("час должен быть в диапазоне 0..23, получено %d", hour)
	}

	return time.Date(day.Year(), day.Month(), day.Day(), hour, 0, 0, 0, time.UTC), nil
}
