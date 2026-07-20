package events

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/KrimsN/gh-pulse/services/gh-collector/internal/model"
)

// newTestClient строит Client для тестов: metrics=nil (см. nil-safe инкременты в events.go),
// logger=nil (значит log.Default()). Тот же приём, что и newTestClient в internal/archive —
// каждый вызов возвращает независимый экземпляр, безопасный под t.Parallel().
func newTestClient() *Client {
	return NewClient("", nil, nil)
}

// TestBackoffInterval — табличный тест на формулу распределения оставшегося запаса запросов на
// оставшееся до сброса время (см. доккомментарий backoffInterval).
func TestBackoffInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		remaining int
		reset     time.Time
		want      time.Duration
	}{
		{
			name:      "запас большой — пауза доли секунды, заведомо меньше X-Poll-Interval",
			remaining: 5000,
			reset:     now.Add(time.Hour),
			want:      time.Hour / 5000,
		},
		{
			name:      "запас почти исчерпан — пауза заметно растёт",
			remaining: 2,
			reset:     now.Add(time.Minute),
			want:      30 * time.Second,
		},
		{
			name:      "remaining=0 — ждём до reset плюс запас",
			remaining: 0,
			reset:     now.Add(10 * time.Second),
			want:      10*time.Second + rateLimitResetMargin,
		},
		{
			name:      "remaining=0 и reset уже в прошлом (рассинхрон часов) — минимальный запас, не отрицательная пауза",
			remaining: 0,
			reset:     now.Add(-time.Minute),
			want:      rateLimitResetMargin,
		},
		{
			name:      "remaining>0, но reset уже в прошлом — не ждать (заголовки устарели, новый запрос сам обновит их)",
			remaining: 100,
			reset:     now.Add(-time.Second),
			want:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := backoffInterval(tt.remaining, tt.reset, now); got != tt.want {
				t.Errorf("backoffInterval(%d, %v, now) = %v, want %v", tt.remaining, tt.reset, got, tt.want)
			}
		})
	}
}

// TestSeenIDsDedup проверяет bounded-множество seenIDs: повтор внутри окна отфильтрован, вытеснение
// самых старых при переполнении освобождает их id для повторного использования (окно, а не факт "id
// вообще когда-либо встречался").
func TestSeenIDsDedup(t *testing.T) {
	t.Parallel()

	s := newSeenIDs(3)

	if s.seenOrAdd(1) {
		t.Fatal("первое появление id=1 не должно считаться дубликатом")
	}
	if !s.seenOrAdd(1) {
		t.Fatal("повторное появление id=1 в пределах окна обязано считаться дубликатом")
	}

	s.seenOrAdd(2)
	s.seenOrAdd(3)
	// Окно (limit=3) теперь [1,2,3]. Добавление 4 обязано вытеснить id=1 (самый старый).
	s.seenOrAdd(4)

	if s.seenOrAdd(4) == false {
		t.Fatal("id=4 только что добавлен — повторное появление обязано считаться дубликатом")
	}
	if s.seenOrAdd(1) {
		t.Fatal("id=1 вытеснен переполнением окна — новое появление не должно считаться дубликатом")
	}
}

// TestSeenIDsHandlesNonMonotonicIDs — регрессия на конкретный факт из реального ответа Events API
// (см. доккомментарий seenIDs): id событий внутри одной страницы не монотонны, поэтому дедуп не
// может опираться на сравнение с максимумом увиденного id.
func TestSeenIDsHandlesNonMonotonicIDs(t *testing.T) {
	t.Parallel()

	s := newSeenIDs(10)
	ids := []uint64{15295311143, 15295311160, 15295311011} // третий меньше первых двух — реальный порядок из ответа API

	for i, id := range ids {
		if s.seenOrAdd(id) {
			t.Fatalf("id[%d]=%d — первое появление, не должно считаться дубликатом", i, id)
		}
	}
	// Меньший id из середины списка (15295311011) обязан считаться дубликатом при повторной
	// встрече — простое "id > last_max_id" пропустило бы его мимо дедупа.
	if !s.seenOrAdd(15295311011) {
		t.Fatal("15295311011 уже встречался — обязан считаться дубликатом, несмотря на то что он не максимум окна")
	}
}

// TestParsePollInterval — табличный тест на разбор X-Poll-Interval.
func TestParsePollInterval(t *testing.T) {
	t.Parallel()

	logger := log.New(io.Discard, "", 0)

	tests := []struct {
		name  string
		input string
		want  time.Duration
	}{
		{name: "обычное значение", input: "60", want: 60 * time.Second},
		{name: "маленькое значение", input: "1", want: time.Second},
		{name: "пусто — заголовка нет вовсе", input: "", want: defaultPollInterval},
		{name: "не число", input: "soon", want: defaultPollInterval},
		{name: "ноль — не валидный интервал поллинга", input: "0", want: defaultPollInterval},
		{name: "отрицательное значение", input: "-5", want: defaultPollInterval},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := parsePollInterval(tt.input, logger); got != tt.want {
				t.Errorf("parsePollInterval(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestParseRateLimitHeaders — табличный тест на разбор X-RateLimit-Remaining/-Reset.
func TestParseRateLimitHeaders(t *testing.T) {
	t.Parallel()

	t.Run("remaining", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			input string
			want  int
		}{
			{"4987", 4987},
			{"0", 0},
			{"", 0},
			{"garbage", 0},
		}
		for _, tt := range tests {
			if got := parseRateLimitRemaining(tt.input); got != tt.want {
				t.Errorf("parseRateLimitRemaining(%q) = %d, want %d", tt.input, got, tt.want)
			}
		}
	})

	t.Run("reset", func(t *testing.T) {
		t.Parallel()
		want := time.Unix(1784459115, 0)
		if got := parseRateLimitReset("1784459115"); !got.Equal(want) {
			t.Errorf("parseRateLimitReset = %v, want %v", got, want)
		}
		if got := parseRateLimitReset("garbage"); !got.IsZero() {
			t.Errorf("parseRateLimitReset(garbage) = %v, want нулевое время", got)
		}
		if got := parseRateLimitReset(""); !got.IsZero() {
			t.Errorf("parseRateLimitReset(\"\") = %v, want нулевое время", got)
		}
	})
}

// TestPollGoldenFixture гоняет полный путь Poll (HTTP + разбор + заголовки) против httptest.Server,
// отдающего testdata/sample_events.json — пять реальных событий, вырезанных байт-в-байт из настоящего
// ответа GET https://api.github.com/events. Проверяет то же, что golden-file тест в internal/archive:
// маппинг верен на форме, которую GitHub отдаёт на самом деле, а не только на сконструированных
// вручную примерах — и что верхнеуровневая форма Events API совпадает с Archive (общий
// model.ParseGitHubEvent).
func TestPollGoldenFixture(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile("testdata/sample_events.json")
	if err != nil {
		t.Fatalf("прочитать фикстуру: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("per_page"); got != strconv.Itoa(perPage) {
			t.Errorf("per_page=%q, want %d", got, perPage)
		}
		w.Header().Set("ETag", `W/"deadbeef"`)
		w.Header().Set("X-Poll-Interval", "60")
		w.Header().Set("X-RateLimit-Remaining", "4987")
		w.Header().Set("X-RateLimit-Reset", "1784459115")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	c := newTestClient()
	c.baseURL = srv.URL

	result, err := c.Poll(t.Context())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if result.NotModified {
		t.Error("NotModified = true на первом 200-ответе")
	}
	if result.PollInterval != 60*time.Second {
		t.Errorf("PollInterval = %v, want 60s", result.PollInterval)
	}
	if result.RateLimitRemaining != 4987 {
		t.Errorf("RateLimitRemaining = %d, want 4987", result.RateLimitRemaining)
	}
	if !result.RateLimitReset.Equal(time.Unix(1784459115, 0)) {
		t.Errorf("RateLimitReset = %v, want %v", result.RateLimitReset, time.Unix(1784459115, 0))
	}
	if c.etag != `W/"deadbeef"` {
		t.Errorf("Client.etag = %q после успешного 200-ответа, want %q", c.etag, `W/"deadbeef"`)
	}

	want := []model.Event{
		{
			EventID: 15295576246, EventType: "PushEvent",
			CreatedAt: time.Date(2026, 7, 19, 10, 40, 53, 0, time.UTC),
			ActorID:   306587440, ActorLogin: "sumitragho",
			RepoID: 1305570304, RepoName: "sumitragho/hcpnng",
			OrgID: 0, PayloadSize: 176, Ref: "refs/heads/main",
		},
		{
			EventID: 15295576094, EventType: "CreateEvent",
			CreatedAt: time.Date(2026, 7, 19, 10, 40, 53, 0, time.UTC),
			ActorID:   277289496, ActorLogin: "uda-lab-agent",
			RepoID: 1290567163, RepoName: "uda-lab/leray-hopf-notes",
			OrgID: 267230770, PayloadSize: 254, Ref: "fix-100-pr97-pr98",
		},
		{
			EventID: 15295576107, EventType: "DeleteEvent",
			CreatedAt: time.Date(2026, 7, 19, 10, 40, 53, 0, time.UTC),
			ActorID:   142466759, ActorLogin: "onehoon",
			RepoID: 1191194334, RepoName: "onehoon/OptiClick",
			OrgID: 0, PayloadSize: 84, Ref: "v0.7.8", // DeleteEvent: payload.ref — короткое имя ("v0.7.8"), не refs/tags/...
		},
		{
			EventID: 11986415484, EventType: "PullRequestEvent",
			CreatedAt: time.Date(2026, 7, 19, 10, 42, 7, 0, time.UTC),
			ActorID:   49699333, ActorLogin: "dependabot[bot]",
			RepoID: 977681048, RepoName: "Yorgoangelopoulos/v0-v2-kripto-bozum-sitesi",
			OrgID: 0, PayloadSize: 627, Ref: "", // PullRequestEvent не в refCarryingEventTypes
		},
		{
			EventID: 11986415402, EventType: "WatchEvent",
			CreatedAt: time.Date(2026, 7, 19, 10, 42, 5, 0, time.UTC),
			ActorID:   121334439, ActorLogin: "brianconlan2023",
			RepoID: 1305624388, RepoName: "brianconlan2023/Youtube-Loop-Music-Video-Generator",
			OrgID: 0, PayloadSize: 20, Ref: "", // WatchEvent = звезда, ref не входит в refCarryingEventTypes
		},
	}

	if len(result.Events) != len(want) {
		t.Fatalf("Poll вернул %d событий, ожидалось %d", len(result.Events), len(want))
	}
	for i := range want {
		got := result.Events[i]
		if !got.CreatedAt.Equal(want[i].CreatedAt) {
			t.Errorf("событие %d: CreatedAt = %v, want %v", i, got.CreatedAt, want[i].CreatedAt)
		}
		gotCmp, wantCmp := got, want[i]
		gotCmp.CreatedAt, wantCmp.CreatedAt = time.Time{}, time.Time{}
		if gotCmp != wantCmp {
			t.Errorf("событие %d:\n got  %+v\n want %+v", i, got, want[i])
		}
	}
}

// TestPollNotModified проверяет ключевой критерий приёмки задачи 2.9: 304 Not Modified не тратит
// впустую тело ответа и не расходует rate-limit сверх того, что уже отдали заголовки. Сервер отдаёт
// реальный набор заголовков на первый запрос (как настоящий GitHub), а на второй — 304 с пустым телом,
// проверяя, что пришёл правильный If-None-Match (доккомментарий задачи: "304 нельзя стабильно
// воспроизвести против живого API — тестируется через httptest.Server").
func TestPollNotModified(t *testing.T) {
	t.Parallel()

	const etag = `W/"8277812154c89d837aea4dd924832f41f7f48ade71adea28c1abae205bfc9f38"`

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("X-Poll-Interval", "60")
		w.Header().Set("X-RateLimit-Remaining", "4986")
		w.Header().Set("X-RateLimit-Reset", "1784459115")

		if n == 1 {
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
			return
		}

		if got := r.Header.Get("If-None-Match"); got != etag {
			t.Errorf("второй запрос: If-None-Match=%q, want %q", got, etag)
		}
		w.WriteHeader(http.StatusNotModified)
		// Тело намеренно не пишем — настоящий 304 тела не несёт.
	}))
	defer srv.Close()

	metrics := NewMetrics(prometheus.NewRegistry())
	c := NewClient("", metrics, nil)
	c.baseURL = srv.URL

	if _, err := c.Poll(t.Context()); err != nil {
		t.Fatalf("первый Poll: %v", err)
	}

	result, err := c.Poll(t.Context())
	if err != nil {
		t.Fatalf("второй Poll (304): %v", err)
	}
	if !result.NotModified {
		t.Error("NotModified = false на 304-ответе")
	}
	if len(result.Events) != 0 {
		t.Errorf("304-ответ вернул %d событий, ожидалось 0 — тело 304 не должно разбираться", len(result.Events))
	}
	if result.RateLimitRemaining != 4986 {
		t.Errorf("RateLimitRemaining на 304 = %d, want 4986 — заголовки rate-limit приходят и на 304", result.RateLimitRemaining)
	}

	if got := testutil.ToFloat64(metrics.NotModified); got != 1 {
		t.Errorf("метрика NotModified = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.PollErrors); got != 0 {
		t.Errorf("метрика PollErrors = %v, want 0 — 304 не ошибка", got)
	}
	if requests.Load() != 2 {
		t.Fatalf("сервер получил %d запросов, ожидалось 2", requests.Load())
	}
}

// TestPollResponseTooLarge проверяет осознанную границу на размер тела ответа (maxResponseBytes,
// критерий приёмки задачи 2.9 про payload_size): тело больше лимита — фатальная ошибка страницы, а не
// молчаливая обрезка и попытка распарсить обрубок.
func TestPollResponseTooLarge(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Poll-Interval", "60")
		w.WriteHeader(http.StatusOK)
		// Валидный JSON-массив, но одна строка внутри длиннее maxResponseBytes целиком — тело
		// заведомо превышает лимит вне зависимости от того, где именно резать.
		_, _ = w.Write([]byte(`[{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"created_at":"2026-01-01T00:00:00Z","payload":{"pad":"`))
		_, _ = w.Write(bytes.Repeat([]byte("x"), maxResponseBytes+1))
		_, _ = w.Write([]byte(`"}}]`))
	}))
	defer srv.Close()

	c := newTestClient()
	c.baseURL = srv.URL

	_, err := c.Poll(t.Context())
	if err == nil {
		t.Fatal("ожидалась ошибка на теле ответа больше maxResponseBytes, получен nil")
	}
}

// TestPollSkipsBrokenEvent проверяет, что одно битое событие внутри иначе валидной страницы не
// проваливает разбор всей страницы — тот же принцип, что и в archive.decodeLines для одной битой
// строки JSON Lines (см. её доккомментарий).
func TestPollSkipsBrokenEvent(t *testing.T) {
	t.Parallel()

	body := `[` +
		`{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:00Z"},` +
		`{"id":"not-a-number","type":"WatchEvent","actor":{"id":2,"login":"b"},"repo":{"id":2,"name":"c/d"},"payload":{},"created_at":"2026-01-01T00:00:01Z"},` +
		`{"id":"2","type":"WatchEvent","actor":{"id":2,"login":"b"},"repo":{"id":2,"name":"c/d"},"payload":{},"created_at":"2026-01-01T00:00:01Z"}` +
		`]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Poll-Interval", "60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	metrics := NewMetrics(prometheus.NewRegistry())
	c := NewClient("", metrics, nil)
	c.baseURL = srv.URL

	result, err := c.Poll(t.Context())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(result.Events) != 2 {
		t.Fatalf("получено %d событий, ожидалось 2 (по обе стороны битого)", len(result.Events))
	}
	if result.Events[0].EventID != 1 || result.Events[1].EventID != 2 {
		t.Errorf("event_id = [%d %d], want [1 2]", result.Events[0].EventID, result.Events[1].EventID)
	}
	if got := testutil.ToFloat64(metrics.EventsSkipped); got != 1 {
		t.Errorf("метрика EventsSkipped = %v, want 1", got)
	}
}

// TestPollUnexpectedStatus проверяет, что неожиданный статус (не 200 и не 304) — ошибка, и что при
// этом заголовки rate-limit всё равно разобраны best-effort (см. доккомментарий PollResult) — это
// то, чем Run пользуется для backoff даже на пути ошибки.
func TestPollUnexpectedStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", "1784459115")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"rate limit exceeded"}`))
	}))
	defer srv.Close()

	metrics := NewMetrics(prometheus.NewRegistry())
	c := NewClient("", metrics, nil)
	c.baseURL = srv.URL

	result, err := c.Poll(t.Context())
	if err == nil {
		t.Fatal("ожидалась ошибка на статусе 403, получен nil")
	}
	if result.RateLimitRemaining != 0 {
		t.Errorf("RateLimitRemaining на ошибке = %d, want 0 (заголовки разобраны несмотря на error)", result.RateLimitRemaining)
	}
	if !result.RateLimitReset.Equal(time.Unix(1784459115, 0)) {
		t.Errorf("RateLimitReset на ошибке = %v, want %v", result.RateLimitReset, time.Unix(1784459115, 0))
	}
	if got := testutil.ToFloat64(metrics.PollErrors); got != 1 {
		t.Errorf("метрика PollErrors = %v, want 1", got)
	}
}

// TestPollSetsAuthorizationHeader проверяет, что токен, переданный в NewClient, идёт в запрос как
// "Authorization: Bearer <token>" — текущий стандарт GitHub REST API (см. критерий приёмки задачи).
func TestPollSetsAuthorizationHeader(t *testing.T) {
	t.Parallel()

	const token = "ghp_test_token"

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("X-Poll-Interval", "60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := NewClient(token, nil, nil)
	c.baseURL = srv.URL

	if _, err := c.Poll(t.Context()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if want := "Bearer " + token; gotAuth != want {
		t.Errorf("Authorization = %q, want %q", gotAuth, want)
	}
}

// TestPollNoTokenOmitsAuthorizationHeader — обратный случай: без токена (неаутентифицированный
// поллинг, лимит 60/час) заголовок Authorization не отправляется вовсе, а не пустой строкой.
func TestPollNoTokenOmitsAuthorizationHeader(t *testing.T) {
	t.Parallel()

	var sawAuthHeader bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawAuthHeader = r.Header["Authorization"]
		w.Header().Set("X-Poll-Interval", "60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := newTestClient()
	c.baseURL = srv.URL

	if _, err := c.Poll(t.Context()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if sawAuthHeader {
		t.Error("заголовок Authorization отправлен без токена")
	}
}

// TestRunDeduplicatesOverlappingPagesAndStopsOnCancel — сквозной тест на Run: соседние
// перекрывающиеся страницы (тот же сценарий, что описан в задаче 2.9 — "одно и то же событие может
// прийти в двух последовательных поллах") не должны задваиваться в out, а отмена ctx должна
// останавливать поллинг и возвращать ctx.Err(), тем же способом, каким это делает
// archive.Client.FetchHour (см. TestOrchestrateGracefulShutdownFlushesReadEvents в cmd/gh-collector).
func TestRunDeduplicatesOverlappingPagesAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	page1 := `[` +
		`{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:00Z"},` +
		`{"id":"2","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:01Z"},` +
		`{"id":"3","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:02Z"}` +
		`]`
	// page2 перекрывается с page1 по id=2,3 (типичная соседняя страница живого потока) и добавляет
	// ровно одно новое событие — id=4.
	page2 := `[` +
		`{"id":"2","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:01Z"},` +
		`{"id":"3","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:02Z"},` +
		`{"id":"4","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:03Z"}` +
		`]`

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		// Поллинг быстрый (1с) и запас лимита большой (5000/час) — backoffInterval не должен
		// доминировать над X-Poll-Interval (см. её доккомментарий): при remaining=5000, reset через
		// час формула даёт ~0.7с, меньше секунды ниже, так что итоговая пауза — ровно 1с.
		w.Header().Set("X-Poll-Interval", "1")
		w.Header().Set("X-RateLimit-Remaining", "5000")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			_, _ = w.Write([]byte(page1))
			return
		}
		// page2 повторяется на каждом следующем запросе — реалистичная имитация "лента почти не
		// сдвинулась между поллами", устойчивый повод убедиться, что дедуп не отпускает уже виденные id.
		_, _ = w.Write([]byte(page2))
	}))
	defer srv.Close()

	c := newTestClient()
	c.baseURL = srv.URL

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	out := make(chan model.Event)
	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(ctx, out)
	}()

	want := []uint64{1, 2, 3, 4}
	got := make([]uint64, 0, len(want))
	for range want {
		select {
		case evt := <-out:
			got = append(got, evt.EventID)
		case <-time.After(10 * time.Second):
			t.Fatalf("не дождались %d-го события за 10с (уже получено %v)", len(got)+1, got)
		}
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event_id[%d] = %d, want %d (got=%v)", i, got[i], want[i], got)
		}
	}

	// Дадим Run сделать ещё пару поллов page2 — если бы дедуп не удерживал состояние между
	// вызовами Run (например, seenIDs пересоздавался бы на каждой итерации), сюда пришли бы
	// повторы id=2,3,4.
	select {
	case evt := <-out:
		t.Fatalf("получено лишнее событие после дедупа: event_id=%d", evt.EventID)
	case <-time.After(700 * time.Millisecond):
		// ожидаемо: новых событий нет, все id из page2 уже видели.
	}

	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run вернул %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run не завершился за 10с после отмены ctx")
	}

	if requests.Load() < 2 {
		t.Fatalf("сервер получил %d запросов, ожидалось хотя бы 2 (page1 + page2)", requests.Load())
	}
}
