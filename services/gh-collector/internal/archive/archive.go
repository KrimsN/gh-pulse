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

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

// baseURL — шаблон адреса почасового дампа GH Archive. Не константа, а переменная: тесты
// подменяют её на адрес httptest.Server, чтобы проверить HTTP-путь (включая 404) без похода
// в реальную сеть.
var baseURL = "https://data.gharchive.org"

// httpClient — свой *http.Client, а не http.DefaultClient: последний — изменяемый пакетный
// глобал, который любой другой код в этом же процессе может подменить или использовать с другими
// ожиданиями по таймаутам.
//
// У клиента намеренно нет общего Timeout: он покрывает всё время запроса вместе с телом ответа,
// а час 2024 года — это больше гигабайта, который легально качается несколько минут даже на
// быстрой сети — общий Timeout оборвал бы такую закачку на середине. Вместо этого ограничены
// только фазы установления соединения и заголовков: если сервер не отвечает вовсе, воркер (в
// задаче 1.4 — из воркер-пула продюсера) не зависает на нём навсегда, но само тело потом читается
// без ограничения по времени — ровно так, как FetchHour и должен работать со стримом.
var httpClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
	},
}

// maxLineSize — верхняя граница длины одной строки JSON Lines в дампе. bufio.Scanner под неё
// выделяет буфер не сразу целиком: старт — 64 КБ (см. NewScanner ниже), и Scanner растит его
// удвоением по мере необходимости вплоть до этого потолка — восемь мегабайт это верхняя граница,
// а не разовая аллокация. Реальные события GH Archive умещаются в единицы-десятки килобайт
// (самый большой замеченный — ForkEvent с вложенным repo, ~5.4 КБ, см. testdata/sample_hour.jsonl),
// так что запас в разы больше нужного — но именно поэтому строка длиннее этого предела означает не
// «редкий большой payload», а вероятную порчу потока, и обрабатывается как фатальная (см. decodeLines).
const maxLineSize = 8 * 1024 * 1024

// URLForHour строит адрес дампа для часа в UTC. GH Archive не дополняет час ведущим нулём —
// используем %d, а не %02d: иначе 2026-06-01 9:00 превратился бы в несуществующий файл
// "...-09.json.gz" вместо настоящего "...-9.json.gz".
func URLForHour(hour time.Time) string {
	h := hour.UTC()
	return fmt.Sprintf("%s/%04d-%02d-%02d-%d.json.gz", baseURL, h.Year(), int(h.Month()), h.Day(), h.Hour())
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
// пропускаются — одна такая строка не должна проваливать чтение всего часа.
//
// out — канал с семантикой backpressure, которую задаёт вызывающий код: если консьюмер (в 1.4 —
// воркер-пул продюсера Kafka) не успевает вычитывать события, отправка в out блокируется, и
// FetchHour просто перестаёт качать данные из сети быстрее, чем их можно обработать. Никакой
// внутренней неограниченной буферизации здесь нет и не должно быть.
//
// FetchHour не закрывает out — это осознанное отступление от идиомы «отправитель закрывает
// канал»: закрытие остаётся на вызывающем коде. В задаче 1.4 несколько вызовов FetchHour (по
// одному на час бэкфилла) будут писать в один общий канал воркер-пула продюсера, и закрыть его
// сможет только тот, кто знает, что закончили все отправители разом (например, через
// sync.WaitGroup вокруг всех вызовов) — сама FetchHour этого не знает и знать не должна.
func FetchHour(ctx context.Context, hour time.Time, out chan<- model.Event) error {
	url := URLForHour(hour)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request for %s: %w", url, err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", url, err)
	}
	// Ошибку закрытия глушим осознанно, а не по недосмотру: тело только читается, и его Close
	// сообщает лишь о сбое возврата соединения в пул — сделать с этим на выходе из функции нечего,
	// а подменять ею настоящую ошибку чтения (её возвращаем ниже) было бы прямым вредом.
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: unexpected status %s", url, resp.Status)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("open gzip stream for %s: %w", url, err)
	}
	// Close у gzip.Reader не дочитывает поток и не проверяет CRC (это делает Read), а лишь
	// освобождает декомпрессор — возвращать отсюда нечего.
	defer func() { _ = gz.Close() }()

	// path.Base(url), а не hour.Format("...-15"): формат с датой всегда даёт двузначный час
	// ("...-09"), а URLForHour час без ведущего нуля ("...-9") — метка в логе обязана совпадать
	// с именем реально запрошенного файла, чтобы по логу можно было грепнуть тот же файл руками.
	label := path.Base(url)
	if err := decodeLines(ctx, gz, label, out); err != nil {
		// Отмена ctx во время сетевого чтения тоже всплывает сюда (http.Transport обрывает
		// соединение по ctx.Done()) — в этом случае возвращаем именно ctx.Err(), чтобы вызывающий
		// код видел штатную остановку, а не произвольную ошибку чтения потока.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
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
func decodeLines(ctx context.Context, r io.Reader, label string, out chan<- model.Event) error {
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
			log.Printf("archive: час %s, строка %d: пропускаю битое событие: %v", label, lineNum, err)
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
