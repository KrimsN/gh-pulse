// Package archive скачивает почасовые дампы GH Archive (gzip, JSON Lines) и стримит их в канал
// нормализованных событий model.Event. Час в распакованном виде — это от ~100 МБ (2026) до
// ~1.1 ГБ (2024, самый крупный измеренный, см. ADR 0007); пакет читает его потоково через
// gzip.Reader и построчный разбор, ни разу не собирая весь час в память целиком (никакого
// io.ReadAll).
package archive

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

// defaultBaseURL — шаблон адреса почасового дампа GH Archive.
const defaultBaseURL = "https://data.gharchive.org"

// maxLineSize — верхняя граница длины одной строки JSON Lines в дампе. bufio.Scanner под неё
// выделяет буфер не сразу целиком: старт — 64 КБ (см. NewClient ниже), и Scanner растит его
// удвоением по мере необходимости вплоть до этого потолка — восемь мегабайт это верхняя граница,
// а не разовая аллокация. Реальные события GH Archive умещаются в единицы-десятки килобайт
// (самый большой замеченный — ForkEvent с вложенным repo, ~5.4 КБ, см. testdata/sample_hour.jsonl),
// так что запас в разы больше нужного — но именно поэтому строка длиннее этого предела означает не
// «редкий большой payload», а вероятную порчу потока, и обрабатывается как фатальная (см. decodeLines).
const maxLineSize = 8 * 1024 * 1024

// Metrics — Prometheus-метрики пакета. Отдельная структура, а не пакетные счётчики: Client
// принимает *Metrics явно (как и логгер), поэтому тесты и несколько воркеров worker pool задачи
// 1.4 не толкаются за один и тот же prometheus.DefaultRegisterer, и повторная регистрация одних и
// тех же метрик в нескольких тестах не паникует.
type Metrics struct {
	FetchErrors  prometheus.Counter
	LinesSkipped prometheus.Counter
}

// NewMetrics создаёт и регистрирует метрики пакета в reg. Вызывать один раз на процесс —
// повторная регистрация тех же имён в том же Registerer паникует (это осознанное поведение
// библиотеки prometheus/client_golang, а не баг: задвоенные метрики молча искажали бы дашборд).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		FetchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_fetch_errors_total",
			Help: "Число часов GH Archive, которые не удалось скачать и разобрать целиком " +
				"(сеть, HTTP-статус не 200, битый gzip, либо строка JSON Lines длиннее maxLineSize).",
		}),
		LinesSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gh_collector_lines_skipped_total",
			Help: "Число строк JSON Lines, пропущенных как битые. Одна битая строка не проваливает " +
				"разбор всего часа — считает, сколько их было, для наблюдаемости качества дампа.",
		}),
	}
	reg.MustRegister(m.FetchErrors, m.LinesSkipped)
	return m
}

// incFetchErrors и incLinesSkipped — nil-safe инкременты. Метод на нулевом (nil) указателе —
// валидный приём в Go, пока тело метода не разыменовывает поля: он позволяет тестам, которым
// метрики не нужны, передавать в NewClient nil вместо настройки Registry, не роняя программу
// проверкой на nil на каждом вызывающем месте (это не то же самое, что паника от вызова метода на
// nil-интерфейсе — здесь получатель нулевой указатель конкретного типа *Metrics, а не интерфейс).
func (m *Metrics) incFetchErrors() {
	if m == nil {
		return
	}
	m.FetchErrors.Inc()
}

func (m *Metrics) incLinesSkipped() {
	if m == nil {
		return
	}
	m.LinesSkipped.Inc()
}

// Client скачивает часы GH Archive. Раньше (задача 1.3) это были пакетные глобалы (baseURL,
// httpClient, log.Printf) — их пришлось свернуть в структуру по двум причинам: воркер-пул задачи
// 1.4 запускает несколько часов конкурентно и им нужно разделяемое, но явно переданное состояние
// (общий httpClient — да, общий логгер и метрики — да), а тесты подменяли baseURL пакетной
// переменной с восстановлением через defer, что несовместимо с t.Parallel() (параллельные тесты
// делят один и тот же глобал и гонятся за его значением). Client вместо этого создаётся с нужным
// baseURL один раз на тест/на процесс — никакого общего мутируемого состояния между вызовами.
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *log.Logger
	metrics    *Metrics
}

// NewClient строит Client для скачивания GH Archive. metrics и logger можно передать nil:
// metrics тогда молчит (см. incFetchErrors/incLinesSkipped выше), logger — заменяется на
// log.Default(), то же поведение, что было у пакетного log.Printf в задаче 1.3.
func NewClient(metrics *Metrics, logger *log.Logger) *Client {
	if logger == nil {
		logger = log.Default()
	}
	return &Client{
		baseURL:    defaultBaseURL,
		httpClient: newHTTPClient(),
		logger:     logger,
		metrics:    metrics,
	}
}

// newHTTPClient строит *http.Client, а не отдаёт http.DefaultClient: последний — изменяемый
// пакетный глобал, который любой другой код в этом же процессе может подменить или использовать с
// другими ожиданиями по таймаутам.
//
// У клиента намеренно нет общего Timeout: он покрывает всё время запроса вместе с телом ответа,
// а час 2024 года — это больше гигабайта, который легально качается несколько минут даже на
// быстрой сети — общий Timeout оборвал бы такую закачку на середине. Вместо этого ограничены
// только фазы установления соединения и заголовков: если сервер не отвечает вовсе, воркер из
// worker pool продюсера (задача 1.4) не зависает на нём навсегда, но само тело потом читается без
// ограничения по времени — ровно так, как FetchHour и должен работать со стримом.
func newHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		},
	}
}

// URLForHour строит адрес дампа для часа в UTC. GH Archive не дополняет час ведущим нулём —
// используем %d, а не %02d: иначе 2026-06-01 9:00 превратился бы в несуществующий файл
// "...-09.json.gz" вместо настоящего "...-9.json.gz".
func (c *Client) URLForHour(hour time.Time) string {
	h := hour.UTC()
	return fmt.Sprintf("%s/%04d-%02d-%02d-%d.json.gz", c.baseURL, h.Year(), int(h.Month()), h.Day(), h.Hour())
}

// FetchHour стримит события одного часа GH Archive в out. Останавливается по ctx: как только
// контекст отменён (например, SIGINT из cmd/gh-collector), функция прекращает чтение на текущей
// строке и возвращает ctx.Err() — вызывающий код отличает штатную остановку от сетевого сбоя
// через errors.Is(err, context.Canceled).
//
// Возвращает ошибку только на фатальном сбое: сеть недоступна, ответ не 200 (в том числе 404 —
// часа не существует), битый gzip-заголовок, либо одна строка JSON Lines длиннее maxLineSize
// (см. decodeLines — это единственный вид "битой строки", который не логируется и не
// пропускается, а прерывает весь час). Остальные повреждённые строки JSON логируются и
// пропускаются — одна такая строка не должна проваливать чтение всего часа. Каждый фатальный
// сбой (кроме отмены ctx — это штатная остановка, а не сбой) увеличивает Metrics.FetchErrors.
//
// out — канал с семантикой backpressure, которую задаёт вызывающий код: если консьюмер (в 1.4 —
// продюсер Kafka) не успевает вычитывать события, отправка в out блокируется, и FetchHour просто
// перестаёт качать данные из сети быстрее, чем их можно обработать. Никакой внутренней
// неограниченной буферизации здесь нет и не должно быть.
//
// FetchHour не закрывает out — это осознанное отступление от идиомы «отправитель закрывает
// канал»: закрытие остаётся на вызывающем коде. Несколько вызовов FetchHour (по одному на час
// бэкфилла, задача 1.4) пишут в один общий канал воркер-пула продюсера, и закрыть его может
// только тот, кто знает, что закончили все отправители разом (см. errgroup.Group в
// cmd/gh-collector/main.go) — сама FetchHour этого не знает и знать не должна.
func (c *Client) FetchHour(ctx context.Context, hour time.Time, out chan<- model.Event) error {
	url := c.URLForHour(hour)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request for %s: %w", url, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.metrics.incFetchErrors()
		return fmt.Errorf("fetch %s: %w", url, err)
	}
	// Ошибку закрытия глушим осознанно, а не по недосмотру: тело только читается, и его Close
	// сообщает лишь о сбое возврата соединения в пул — сделать с этим на выходе из функции нечего,
	// а подменять ею настоящую ошибку чтения (её возвращаем ниже) было бы прямым вредом.
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		c.metrics.incFetchErrors()
		return fmt.Errorf("fetch %s: unexpected status %s", url, resp.Status)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		c.metrics.incFetchErrors()
		return fmt.Errorf("open gzip stream for %s: %w", url, err)
	}
	// Close у gzip.Reader не дочитывает поток и не проверяет CRC (это делает Read), а лишь
	// освобождает декомпрессор — возвращать отсюда нечего.
	defer func() { _ = gz.Close() }()

	// path.Base(url), а не hour.Format("...-15"): формат с датой всегда даёт двузначный час
	// ("...-09"), а URLForHour час без ведущего нуля ("...-9") — метка в логе обязана совпадать
	// с именем реально запрошенного файла, чтобы по логу можно было грепнуть тот же файл руками.
	label := path.Base(url)
	if err := c.decodeLines(ctx, gz, label, out); err != nil {
		// Отмена ctx во время сетевого чтения тоже всплывает сюда (http.Transport обрывает
		// соединение по ctx.Done()) — в этом случае возвращаем именно ctx.Err(), чтобы вызывающий
		// код видел штатную остановку, а не произвольную ошибку чтения потока. Метрику
		// FetchErrors в этом случае не трогаем: отмена — не сбой.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		c.metrics.incFetchErrors()
		return fmt.Errorf("%s: %w", url, err)
	}
	return nil
}

// decodeLines разбирает JSON Lines построчно и отправляет нормализованные события в out.
// Вынесена отдельно от FetchHour ради тестируемости: golden-file тест подаёт сюда фикстуру
// напрямую через os.File, не поднимая ни настоящий HTTP-запрос, ни gzip.
//
// bufio.Scanner, а не json.Decoder напрямую поверх r: после ошибки Decode() позиция в потоке
// произвольная (декодер мог откусить только часть битого токена), и продолжать вызывать Decode()
// для следующих объектов небезопасно — велик риск словить каскад ошибок до конца файла. Формат
// GH Archive — JSON Lines: ровно один объект на строку, поэтому Scanner даёт естественную точку
// ресинхронизации — начало следующей строки, и одна битая (но не длиннее maxLineSize) строка не
// выбивает из потока весь оставшийся час.
//
// Исключение — строка длиннее maxLineSize: Scanner в этом случае не возвращает её вовсе, а
// прекращает работу с bufio.ErrTooLong (Scan() возвращает false, Err() — ErrTooLong). Ресинхро-
// низация "начало следующей строки", ради которой выбран Scanner, здесь не работает: Scanner не
// умеет отбросить остаток слишком длинной строки и продолжить как ни в чём не бывало — он просто
// останавливается. Такую строку мы намеренно НЕ пытаемся пропустить и продолжить: это осознанный
// выбор в пользу простоты и предсказуемости, а не полноты — при реальных объёмах события GH Archive
// (единицы-десятки КБ, см. maxLineSize) строка такой длины означает порчу потока, а не большой
// payload, и трактуется как фатальный сбой всего часа.
func (c *Client) decodeLines(ctx context.Context, r io.Reader, label string, out chan<- model.Event) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	lineNum := 0
	for scanner.Scan() {
		lineNum++

		line := scanner.Bytes()
		if len(line) == 0 {
			// Пустая строка — не событие и не повреждённые данные, а просто лишний перевод
			// строки (например, последний \n файла). Логировать её как битую было бы шумом.
			continue
		}

		evt, err := model.ParseGitHubEvent(line)
		if err != nil {
			c.metrics.incLinesSkipped()
			c.logger.Printf("archive: час %s, строка %d: пропускаю битое событие: %v", label, lineNum, err)
			continue
		}

		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return fmt.Errorf("час %s, строка %d длиннее %d байт (maxLineSize) — трактуется как фатальный сбой, не как битая строка: %w",
				label, lineNum+1, maxLineSize, err)
		}
		return err
	}
	return nil
}
