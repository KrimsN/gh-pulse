// cmd/gh-collector — точка входа коллектора. На этапе задачи 1.3 умеет одно: скачать один час
// GH Archive, стримово нормализовать события и напечатать счётчик и небольшой сэмпл. Kafka-
// продюсер, воркер-пул по нескольким часам, флаги --backfill/--workers и метрики — задача 1.4.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/archive"
	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

// errUsage — сигнальная ошибка неверных флагов/аргументов. main отличает её от сбоя fetch через
// errors.Is, чтобы вернуть exit code 2 (конвенция CLI-утилит: 2 = неверное использование), а не 1.
var errUsage = errors.New("usage")

func main() {
	err := run()
	switch {
	case err == nil:
		return
	case errors.Is(err, errUsage):
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(os.Stderr, "остановлено пользователем (SIGINT), час докачан не полностью")
		os.Exit(130) // конвенция shell: 128 + SIGINT(2)
	default:
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run содержит всю логику команды и возвращает error вместо прямого os.Exit/log.Fatalf. Это не
// стилистическая прихоть: os.Exit и log.Fatal обрывают процесс немедленно и пропускают все defer
// в стеке вызовов. Сейчас единственный defer здесь — signal.NotifyContext-овский stop(), и его
// пропуск безвреден. Но в задаче 1.4 на этом месте появится defer с флашем батчей в Kafka перед
// выходом — и если тело команды по-прежнему звало бы os.Exit напрямую, флаш молча не сработал бы
// на любом пути завершения. run() возвращает управление в main естественно (без os.Exit внутри),
// поэтому все defer гарантированно отрабатывают до того, как main решит код возврата.
func run() error {
	hourFlag := flag.String("hour", "", "час GH Archive в UTC, формат YYYY-MM-DD-H, например 2026-06-01-15")
	sampleN := flag.Int("sample", 3, "сколько первых событий распечатать как сэмпл")
	flag.Parse()

	if *hourFlag == "" {
		return fmt.Errorf("%w: нужен флаг --hour, например --hour=2026-06-01-15", errUsage)
	}
	if *sampleN < 0 {
		return fmt.Errorf("%w: --sample не может быть отрицательным, получено %d", errUsage, *sampleN)
	}

	hour, err := parseHour(*hourFlag)
	if err != nil {
		// Два %w, а не %w + %v: Go 1.20+ умеет заворачивать несколько ошибок сразу. errUsage нужен
		// main для кода возврата 2, но и причина от parseHour должна остаться доступной errors.Is —
		// с %v она превратилась бы в текст и перестала быть ошибкой.
		return fmt.Errorf("%w: разбор --hour: %w", errUsage, err)
	}

	// SIGINT отменяет корневой context: FetchHour остановится на текущей строке, не пытаясь
	// докачать оставшуюся часть часа. Полноценный graceful shutdown с флашем батчей в Kafka —
	// задача 1.4; здесь это тот же примитив (context насквозь), но без чего флашить.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Буферизованный канал: FetchHour может уйти вперёд печати сэмпла на несколько сотен событий,
	// не блокируясь после каждой строки. В задаче 1.4 этот же канал станет входом воркер-пула
	// Kafka-продюсера — размер буфера здесь уже задаёт форму backpressure между fetch и produce.
	events := make(chan model.Event, 1000)

	var fetchErr error
	go func() {
		fetchErr = archive.FetchHour(ctx, hour, events)
		close(events) // сигнал консьюмеру ниже: событий больше не будет
	}()

	count := 0
	samples := make([]model.Event, 0, *sampleN)
	for evt := range events {
		count++
		if len(samples) < *sampleN {
			samples = append(samples, evt)
		}
	}
	// Закрытие events happens-before выход из range по гарантии рантайма для чтения из
	// закрытого канала, а close(events) в горутине выше идёт строго после присваивания fetchErr —
	// поэтому здесь fetchErr уже виден без дополнительной синхронизации (WaitGroup тут избыточен).

	for _, s := range samples {
		fmt.Printf("sample: %+v\n", s)
	}
	fmt.Printf("hour=%s events=%d\n", *hourFlag, count)

	if fetchErr != nil {
		return fmt.Errorf("fetch hour %s: %w", *hourFlag, fetchErr)
	}
	return nil
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
	if hour < 0 || hour > 23 {
		return time.Time{}, fmt.Errorf("час должен быть в диапазоне 0..23, получено %d", hour)
	}

	return time.Date(day.Year(), day.Month(), day.Day(), hour, 0, 0, 0, time.UTC), nil
}
