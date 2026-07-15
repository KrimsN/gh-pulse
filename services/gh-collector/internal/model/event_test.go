package model

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// eventsEqual сравнивает два Event без риска, свойственного time.Time и оператору ==:
// два значения, представляющие один и тот же момент времени, не обязаны быть побитово равны
// (могут отличаться внутренним монотонным счётчиком или деталями представления location).
// Идиоматичное сравнение — time.Time.Equal для CreatedAt и == для остальных полей, которые
// у Event все скалярные (uint64/string) и такого риска не несут.
func eventsEqual(a, b Event) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return false
	}
	a.CreatedAt, b.CreatedAt = time.Time{}, time.Time{}
	return a == b
}

// TestParseGitHubEvent — табличный тест маппинга «сырое событие GitHub → каноническая схема».
// Случаи покрывают обе эпохи payload PushEvent (ADR 0007), отсутствующий и присутствующий org,
// типы без ref, payload:null и невалидный/усечённый вход.
func TestParseGitHubEvent(t *testing.T) {
	forkPayload := `{"action":"forked","ref":"this-should-be-ignored"}`

	tests := []struct {
		name          string
		input         string
		want          Event
		wantErr       bool
		skipPayloadSz bool // для случаев, где PayloadSize не суть теста (проверяется golden-file тестом в internal/archive)
	}{
		{
			name: "PushEvent новой эпохи без org",
			input: `{"id":"12660541189","type":"PushEvent","actor":{"id":280740748,"login":"francods9-tech"},` +
				`"repo":{"id":1251357380,"name":"francods9-tech/customers"},` +
				`"payload":{"repository_id":1251357380,"push_id":34950197608,"ref":"refs/heads/main",` +
				`"head":"961e9b9be3607af7772e80181227fef92cf67076","before":"8475abe0ec483ea5c0697e2ab9a3dd595fa3f3df"},` +
				`"public":true,"created_at":"2026-06-01T15:00:00Z"}`,
			want: Event{
				EventID:     12660541189,
				EventType:   "PushEvent",
				CreatedAt:   time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
				ActorID:     280740748,
				ActorLogin:  "francods9-tech",
				RepoID:      1251357380,
				RepoName:    "francods9-tech/customers",
				OrgID:       0,
				Language:    "",
				PayloadSize: 176, // сверено независимым скриптом на реальной строке, см. internal/archive/testdata/sample_hour.jsonl
				Ref:         "refs/heads/main",
			},
		},
		{
			name: "PushEvent старой эпохи с commits/size/distinct_size — не должен ломаться",
			input: `{"id":"54715645747","type":"PushEvent","actor":{"id":45889833,"login":"imyhacker"},` +
				`"repo":{"id":851945034,"name":"imyhacker/hijaw"},` +
				`"payload":{"repository_id":851945034,"push_id":26818913606,"size":1,"distinct_size":1,` +
				`"ref":"refs/heads/master","head":"572b788146a766bed75ee2445e49e2cb87c6dc5c",` +
				`"before":"e7e0c0d84a6f175abe17e3d365cb7d8c3ea15ffd",` +
				`"commits":[{"sha":"572b788146a766bed75ee2445e49e2cb87c6dc5c","distinct":true}]},` +
				`"public":true,"created_at":"2025-09-15T15:00:00Z"}`,
			want: Event{
				EventID:    54715645747,
				EventType:  "PushEvent",
				CreatedAt:  time.Date(2025, 9, 15, 15, 0, 0, 0, time.UTC),
				ActorID:    45889833,
				ActorLogin: "imyhacker",
				RepoID:     851945034,
				RepoName:   "imyhacker/hijaw",
				OrgID:      0,
				Ref:        "refs/heads/master",
			},
			skipPayloadSz: true, // старая форма payload здесь усечена вручную (без полного commits) — размер не показателен
		},
		{
			name: "событие с org",
			input: `{"id":"12660541222","type":"PushEvent","actor":{"id":170764530,"login":"gortiz-dotcms"},` +
				`"repo":{"id":3729629,"name":"dotCMS/core"},` +
				`"payload":{"ref":"refs/heads/main"},"created_at":"2026-06-01T15:00:00Z",` +
				`"org":{"id":1005263,"login":"dotCMS"}}`,
			want: Event{
				EventID:    12660541222,
				EventType:  "PushEvent",
				CreatedAt:  time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
				ActorID:    170764530,
				ActorLogin: "gortiz-dotcms",
				RepoID:     3729629,
				RepoName:   "dotCMS/core",
				OrgID:      1005263,
				Ref:        "refs/heads/main",
			},
			skipPayloadSz: true, // payload здесь урезан до одного ref ради краткости кейса — размер не тот, что в реальном событии
		},
		{
			name: "WatchEvent — звезда, ref в payload нет и не должен подставляться",
			input: `{"id":"10136123952","type":"WatchEvent","actor":{"id":31294465,"login":"Degasde"},` +
				`"repo":{"id":1240022612,"name":"denislupookov/altersend"},` +
				`"payload":{"action":"started"},"created_at":"2026-06-01T15:00:08Z"}`,
			want: Event{
				EventID:     10136123952,
				EventType:   "WatchEvent",
				CreatedAt:   time.Date(2026, 6, 1, 15, 0, 8, 0, time.UTC),
				ActorID:     31294465,
				ActorLogin:  "Degasde",
				RepoID:      1240022612,
				RepoName:    "denislupookov/altersend",
				OrgID:       0,
				PayloadSize: 20, // ровно длина {"action":"started"} — совпадает с фактом из ADR 0007 (WatchEvent всегда 20 байт)
				Ref:         "",
			},
		},
		{
			name: "ForkEvent — тип без ref, даже если бы payload его случайно содержал",
			input: fmt.Sprintf(`{"id":"1","type":"ForkEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},`+
				`"payload":%s,"created_at":"2026-01-01T00:00:00Z"}`, forkPayload),
			want: Event{
				EventID:     1,
				EventType:   "ForkEvent",
				CreatedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				ActorID:     1,
				ActorLogin:  "a",
				RepoID:      1,
				RepoName:    "a/b",
				OrgID:       0,
				PayloadSize: uint32(len(forkPayload)), // считаем длину той же строкой, что подставили в payload — не переносим число руками
				Ref:         "",                       // ForkEvent не входит в refCarryingEventTypes: поле payload.ref, даже существуя, игнорируется
			},
		},
		{
			name: "payload:null — валидный JSON, PayloadSize обязан быть 0, а не 4 (длина литерала null)",
			input: `{"id":"1","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},` +
				`"payload":null,"created_at":"2026-01-01T00:00:00Z"}`,
			want: Event{
				EventID:     1,
				EventType:   "WatchEvent",
				CreatedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				ActorID:     1,
				ActorLogin:  "a",
				RepoID:      1,
				RepoName:    "a/b",
				OrgID:       0,
				PayloadSize: 0,
				Ref:         "",
			},
		},
		{
			name:    "нечисловой id — ошибка, а не паника",
			input:   `{"id":"not-a-number","type":"WatchEvent","actor":{"id":1,"login":"a"},"repo":{"id":1,"name":"a/b"},"payload":{},"created_at":"2026-01-01T00:00:00Z"}`,
			wantErr: true,
		},
		{
			name:    "битый JSON — ошибка, а не паника",
			input:   `{"id":"1","type":`,
			wantErr: true,
		},
		{
			name:    "нет type — усечённая строка не должна пройти как событие с пустым типом",
			input:   `{"id":"7","created_at":"2026-01-01T00:00:00Z"}`,
			wantErr: true,
		},
		{
			name:    "нет created_at — иначе событие получило бы created_at года 1 и сломало PARTITION BY toYYYYMM в ClickHouse",
			input:   `{"id":"7","type":"WatchEvent"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGitHubEvent([]byte(tt.input))

			if tt.wantErr {
				if err == nil {
					t.Fatalf("ожидалась ошибка, получено событие %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}

			if tt.skipPayloadSz {
				tt.want.PayloadSize = got.PayloadSize
			}

			if !eventsEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestEventRoundTrip проверяет, что Event — симметричный DTO: json.Marshal, а следом
// json.Unmarshal того же среза обратно в Event, обязаны дать исходное значение. Это ровно то
// требование, которое нарушал прежний Event.UnmarshalJSON, заточенный под сырой формат GitHub
// (event_id-строка, вложенные actor/repo/org), а не под собственный канонический вид Event
// (event_id-число, плоская структура). Event едет в Kafka через json.Marshal (задача 1.4) —
// любой Go-код, читающий топик gh.events обратно, полагается именно на эту симметрию.
func TestEventRoundTrip(t *testing.T) {
	original := Event{
		EventID:     12660541222,
		EventType:   "PushEvent",
		CreatedAt:   time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
		ActorID:     170764530,
		ActorLogin:  "gortiz-dotcms",
		RepoID:      3729629,
		RepoName:    "dotCMS/core",
		OrgID:       1005263,
		Language:    "",
		PayloadSize: 217,
		Ref:         "refs/heads/issue-35647-create-new-lang-version-dialog-error",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var roundTripped Event
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("Unmarshal(Marshal(original)) вернул ошибку вместо исходного значения: %v\nJSON: %s", err, data)
	}

	if !eventsEqual(original, roundTripped) {
		t.Errorf("round-trip не сохранил значение:\n original     %+v\n roundTripped %+v", original, roundTripped)
	}
}
