package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

// newTestClient строит Client для тестов: metrics=nil (тестам метрики не нужны, см. nil-safe
// инкременты в archive.go), logger=nil (значит log.Default() — тесты его не проверяют). Каждый
// вызов возвращает независимый экземпляр, поэтому тесты, меняющие baseURL под конкретный
// httptest.Server, не делят состояние друг с другом и безопасны под t.Parallel() — в отличие от
// прежней пакетной переменной baseURL с восстановлением через defer.
func newTestClient() *Client {
	return NewClient(nil, nil)
}

func TestURLForHour(t *testing.T) {
	t.Parallel()

	// Табличный тест на форматирование адреса. Ключевой нюанс — час без ведущего нуля
	// (GH Archive пишет "...-9.json.gz", а не "...-09.json.gz"), остальное — обычный %04d/%02d.
	tests := []struct {
		name string
		hour time.Time
		want string
	}{
		{
			name: "полдень с двузначным часом",
			hour: time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
			want: "https://data.gharchive.org/2026-06-01-15.json.gz",
		},
		{
			name: "однозначный час без ведущего нуля",
			hour: time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC),
			want: "https://data.gharchive.org/2026-06-01-9.json.gz",
		},
		{
			name: "полночь — час 0",
			hour: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
			want: "https://data.gharchive.org/2026-01-05-0.json.gz",
		},
		{
			name: "время не в UTC приводится к UTC",
			hour: time.Date(2026, 6, 1, 18, 0, 0, 0, time.FixedZone("MSK", 3*3600)), // 18:00 MSK = 15:00 UTC
			want: "https://data.gharchive.org/2026-06-01-15.json.gz",
		},
	}

	c := newTestClient()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := c.URLForHour(tt.hour); got != tt.want {
				t.Errorf("URLForHour(%v) = %q, want %q", tt.hour, got, tt.want)
			}
		})
	}
}

// TestDecodeLinesGolden — golden-file тест на testdata/sample_hour.jsonl: семь реальных событий,
// вырезанных из настоящих часов GH Archive (2026-06-01-15 и 2025-09-15-15, см. комментарий к
// файлу). Проверяет как раз то, что не проверить unit-тестами internal/model в отрыве от реальных
// данных — что маппинг верен на форме, которую GitHub отдаёт на самом деле, а не только на
// сконструированных вручную примерах.
func TestDecodeLinesGolden(t *testing.T) {
	t.Parallel()

	f, err := os.Open("testdata/sample_hour.jsonl")
	if err != nil {
		t.Fatalf("открыть фикстуру: %v", err)
	}
	defer func() { _ = f.Close() }()

	c := newTestClient()

	// Без буфера и в отдельной горутине: decodeLines и чтение из out идут конкурентно, поэтому
	// корректность не зависит от того, сколько событий во фикстуре — при вызове decodeLines
	// синхронно в этой же горутине (как было раньше) канал пришлось бы делать не меньше числа
	// событий, иначе decodeLines заблокировался бы на первой же отправке без читателя.
	out := make(chan model.Event)
	var decodeErr error
	go func() {
		decodeErr = c.decodeLines(context.Background(), f, "test", out)
		close(out)
	}()

	var got []model.Event
	for evt := range out {
		got = append(got, evt)
	}
	if decodeErr != nil {
		t.Fatalf("decodeLines: %v", decodeErr)
	}

	want := []model.Event{
		{
			EventID: 12660541189, EventType: "PushEvent",
			CreatedAt: time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
			ActorID:   280740748, ActorLogin: "francods9-tech",
			RepoID: 1251357380, RepoName: "francods9-tech/customers",
			OrgID: 0, PayloadSize: 176, Ref: "refs/heads/main",
		},
		{
			EventID: 12660541222, EventType: "PushEvent",
			CreatedAt: time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
			ActorID:   170764530, ActorLogin: "gortiz-dotcms",
			RepoID: 3729629, RepoName: "dotCMS/core",
			OrgID: 1005263, PayloadSize: 217, Ref: "refs/heads/issue-35647-create-new-lang-version-dialog-error",
		},
		{
			EventID: 12660541268, EventType: "CreateEvent",
			CreatedAt: time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
			ActorID:   221744039, ActorLogin: "azizbek-coderdev",
			RepoID: 1256205501, RepoName: "azizbek-coderdev/auto-repo-185-1780325996769-kvk6",
			OrgID: 0, PayloadSize: 143, Ref: "main", // у CreateEvent payload.ref — короткое имя ветки, не полный refs/heads/...
		},
		{
			EventID: 12660542862, EventType: "DeleteEvent",
			CreatedAt: time.Date(2026, 6, 1, 15, 0, 1, 0, time.UTC),
			ActorID:   41898282, ActorLogin: "github-actions[bot]",
			RepoID: 1055542783, RepoName: "shahzadhaider1/grafana",
			OrgID: 0, PayloadSize: 126, Ref: "backport-90588-to-v11.1.x",
		},
		{
			EventID: 10136123952, EventType: "WatchEvent",
			CreatedAt: time.Date(2026, 6, 1, 15, 0, 8, 0, time.UTC),
			ActorID:   31294465, ActorLogin: "Degasde",
			RepoID: 1240022612, RepoName: "denislupookov/altersend",
			OrgID: 0, PayloadSize: 20, Ref: "", // WatchEvent = звезда; ref не входит в refCarryingEventTypes
		},
		{
			EventID: 10136125486, EventType: "ForkEvent",
			CreatedAt: time.Date(2026, 6, 1, 15, 0, 8, 0, time.UTC),
			ActorID:   56793572, ActorLogin: "neutrino1961",
			RepoID: 1254736007, RepoName: "Signal-Matrix-Core/trading-bot",
			OrgID: 289302263, PayloadSize: 5428, Ref: "", // ForkEvent тоже без ref, несмотря на большой вложенный payload
		},
		{
			EventID: 54715645747, EventType: "PushEvent",
			CreatedAt: time.Date(2025, 9, 15, 15, 0, 0, 0, time.UTC),
			ActorID:   45889833, ActorLogin: "imyhacker",
			RepoID: 851945034, RepoName: "imyhacker/hijaw",
			OrgID: 0, PayloadSize: 479, Ref: "refs/heads/master", // старая эпоха: payload с commits/size/distinct_size (ADR 0007)
		},
	}

	if len(got) != len(want) {
		t.Fatalf("decodeLines вернул %d событий, ожидалось %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].CreatedAt.Equal(want[i].CreatedAt) {
			t.Errorf("событие %d: CreatedAt = %v, want %v", i, got[i].CreatedAt, want[i].CreatedAt)
		}
		gotCmp, wantCmp := got[i], want[i]
		gotCmp.CreatedAt, wantCmp.CreatedAt = time.Time{}, time.Time{}
		if gotCmp != wantCmp {
			t.Errorf("событие %d:\n got  %+v\n want %+v", i, got[i], want[i])
		}
	}
}

// TestDecodeLinesSkipsBrokenLine проверяет, что одна битая строка не прерывает разбор остальных —
// строка логируется и пропускается, а не валит decodeLines целиком (само описание FetchHour).
func TestDecodeLinesSkipsBrokenLine(t *testing.T) {
	t.Parallel()

	input := "" +
		`{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:00Z"}` + "\n" +
		`this line is not json at all` + "\n" +
		`{"id":"2","type":"WatchEvent","actor":{"id":2,"login":"b"},"repo":{"id":2,"name":"c/d"},"payload":{},"created_at":"2026-01-01T00:00:01Z"}` + "\n"

	c := newTestClient()

	out := make(chan model.Event) // без буфера, decodeLines в отдельной горутине — см. комментарий в TestDecodeLinesGolden
	var decodeErr error
	go func() {
		decodeErr = c.decodeLines(context.Background(), bytes.NewBufferString(input), "test", out)
		close(out)
	}()

	var ids []uint64
	for evt := range out {
		ids = append(ids, evt.EventID)
	}
	if decodeErr != nil {
		t.Fatalf("decodeLines: %v", decodeErr)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("ожидались события [1 2] по обе стороны битой строки, получено %v", ids)
	}
}

// TestDecodeLinesRespectsCancellation проверяет, что при отменённом ctx decodeLines не блокируется
// навечно, пытаясь отправить событие в канал без читателя, а сразу возвращает ctx.Err().
func TestDecodeLinesRespectsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменяем заранее — имитируем SIGINT, пришедший до старта чтения

	input := `{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:00Z"}` + "\n"

	c := newTestClient()

	out := make(chan model.Event) // без буфера и без читателя — при живом ctx это тест бы завис
	err := c.decodeLines(ctx, bytes.NewBufferString(input), "test", out)
	if !errors.Is(err, ctx.Err()) {
		t.Errorf("decodeLines вернул %v, want %v", err, ctx.Err())
	}
}

// TestDecodeLinesBlocksOnFullChannel — прямая проверка backpressure на уровне decodeLines
// (задача 1.4, критерий приёмки «при остановленном/медленном Kafka память коллектора не растёт
// безгранично»): decodeLines не имеет собственного внутреннего буфера сверх того, что дал
// вызывающий код в out. Если консьюмер перестал читать, decodeLines обязана застрять на select
// внутри цикла, а не копить события где-то ещё, пока канал не примет следующее.
func TestDecodeLinesBlocksOnFullChannel(t *testing.T) {
	t.Parallel()

	// Десять строк с запасом — достаточно, чтобы гарантированно упереться в буфер размера 1
	// и остаться с непрочитанными событиями во входном потоке.
	var b strings.Builder
	for i := 1; i <= 10; i++ {
		b.WriteString(`{"id":"`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},` +
			`"payload":{},"created_at":"2026-01-01T00:00:00Z"}` + "\n")
	}

	c := newTestClient()

	out := make(chan model.Event, 1) // буфер на одно событие — переполняется сразу после первого чтения
	done := make(chan error, 1)
	go func() {
		done <- c.decodeLines(context.Background(), strings.NewReader(b.String()), "test", out)
	}()

	// Вычитываем ровно одно событие — буфер снова полон (decodeLines успела положить туда
	// следующее), а читателя для второго уже нет. decodeLines обязана заблокироваться на select
	// внутри цикла и не завершиться, пока событий во входе ещё много.
	<-out

	select {
	case err := <-done:
		t.Fatalf("decodeLines завершилась (err=%v), хотя канал переполнен и никто больше не читает — "+
			"backpressure не работает, события буферизуются где-то помимо канала", err)
	case <-time.After(100 * time.Millisecond):
		// ожидаемо: decodeLines блокируется на отправке в out, backpressure работает.
	}
}

// TestFetchHourHTTP гоняет полный путь FetchHour (HTTP + gzip) против httptest.Server,
// отдающего ту же фикстуру в сжатом виде — без похода в реальную сеть.
func TestFetchHourHTTP(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile("testdata/sample_hour.jsonl")
	if err != nil {
		t.Fatalf("прочитать фикстуру: %v", err)
	}

	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	if _, err := w.Write(fixture); err != nil {
		t.Fatalf("сжать фикстуру: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("закрыть gzip writer: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(gz.Bytes())
	}))
	defer srv.Close()

	// baseURL — поле экземпляра, созданного этим тестом, а не пакетная переменная: другой
	// параллельный тест не увидит и не сможет перезаписать это значение.
	c := newTestClient()
	c.baseURL = srv.URL

	// FetchHour не закрывает out сама (см. её доккомментарий) — закрываем здесь, в горутине,
	// сразу после того, как она вернулась. Раньше тест читал из out с "break" на седьмом событии
	// и без закрытия канала: если бы регрессия отдавала на одно событие меньше, тест не упал бы,
	// а завис бы навсегда в ожидании события, которого больше не будет. Теперь канал закрывается
	// вне зависимости от того, сколько событий фактически отправлено, — get range всегда
	// завершается сам, а неверное количество проверяется уже после цикла.
	out := make(chan model.Event, 16)
	var fetchErr error
	go func() {
		fetchErr = c.FetchHour(context.Background(), time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC), out)
		close(out)
	}()

	count := 0
	for range out {
		count++
	}

	if fetchErr != nil {
		t.Fatalf("FetchHour: %v", fetchErr)
	}
	if count != 7 {
		t.Errorf("получено %d событий, ожидалось 7", count)
	}
}

// TestFetchHourNotFound проверяет обязательное требование: несуществующий час (404) возвращает
// ошибку, а не паникует и не зависает.
func TestFetchHourNotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := newTestClient()
	c.baseURL = srv.URL

	out := make(chan model.Event, 1)
	err := c.FetchHour(context.Background(), time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), out)
	if err == nil {
		t.Fatal("ожидалась ошибка на 404, получен nil")
	}
}
