// Package model содержит каноническую схему события GitHub — единый внутренний контракт
// между gh-collector, Kafka и остальными сервисами GH Pulse (pulse-consumer, pulse-api).
// Обе эпохи формата GH Archive (см. docs/adr/0007-hybrid-data-epochs.md) и GitHub Events API
// нормализуются к одной и той же структуре Event через ParseGitHubEvent.
package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Event — нормализованное событие GitHub, готовое к вставке в ClickHouse (см. миграцию
// infra/clickhouse/migrations/001_events.sql — источник истины по типам колонок).
//
// Event — симметричный DTO: json.Marshal(Event{...}) и json.Unmarshal того же среза обратно в
// Event дают исходное значение (см. TestEventRoundTrip). Это осознанный выбор, а не то же самое,
// что разбор сырого события GitHub, — для последнего есть отдельная ParseGitHubEvent. Одна
// структура не тянет обе роли: Event едет в Kafka через обычный json.Marshal (задача 1.4) и
// должна читаться обратно тем же стандартным json.Unmarshal — любым Go-кодом, который смотрит в
// топик gh.events (интеграционный тест, DLQ-реплей, отладочный consumer), без знания про формат
// проводов GitHub. Если бы Event сам реализовывал json.Unmarshaler под формат GitHub (как было до
// этой правки), Marshal(Event) → Unmarshal(&Event) ломался бы: канонический вид не совпадает с
// сырым (event_id — число, а не строка; нет .actor/.repo/.org как вложенных объектов).
type Event struct {
	EventID     uint64    `json:"event_id"`
	EventType   string    `json:"event_type"`
	CreatedAt   time.Time `json:"created_at"`
	ActorID     uint64    `json:"actor_id"`
	ActorLogin  string    `json:"actor_login"`
	RepoID      uint64    `json:"repo_id"`
	RepoName    string    `json:"repo_name"`
	OrgID       uint64    `json:"org_id"`       // 0 = событие вне организации; НЕ nullable — зеркалит колонку ClickHouse
	Language    string    `json:"language"`     // всегда "" на этом этапе; заполняется обогащением (задача 4.3)
	PayloadSize uint32    `json:"payload_size"` // длина сырого .payload в байтах; см. ADR 0007 про несопоставимость между эпохами
	Ref         string    `json:"ref"`          // .payload.ref — только Push/Create/DeleteEvent, иначе ""
}

// refCarryingEventTypes — типы событий, у payload которых есть поле ref (docs/ARCHITECTURE.md).
// У остальных типов ref в payload просто нет — оставляем Event.Ref пустой строкой, а не пытаемся
// его найти.
var refCarryingEventTypes = map[string]bool{
	"PushEvent":   true,
	"CreateEvent": true,
	"DeleteEvent": true,
}

// githubEvent — форма события ровно такая, какую отдают и GH Archive, и GitHub Events API:
// .id приходит строкой, .org отсутствует у большинства событий (~86%, см. «Сквозные соглашения»).
// Это неэкспортируемый тип формата проводов: наружу из пакета торчит только канонический Event,
// которого он ни в коей мере не заменяет (см. комментарий к Event про раздельные роли).
type githubEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
	Actor     struct {
		ID    uint64 `json:"id"`
		Login string `json:"login"`
	} `json:"actor"`
	Repo struct {
		ID   uint64 `json:"id"`
		Name string `json:"name"`
	} `json:"repo"`
	// Org — указатель, а не встроенная структура: так nil отличим от «организация с id 0»
	// (такой организации в GitHub не существует, но семантику «поля не было» лучше не терять
	// уже на этапе разбора JSON, даже если на выходе Event всё равно схлопывает это в 0).
	Org *struct {
		ID uint64 `json:"id"`
	} `json:"org"`
	// json.RawMessage — payload не разбираем целиком: его форма зависит от типа события и эпохи
	// данных (ADR 0007), а нам из него нужны только длина в байтах и (для части типов) ref.
	// RawMessage хранит ровно те байты, что были в исходном документе, поэтому len(Payload)
	// действительно "длина сырого .payload", а не длина после повторной сериализации.
	Payload json.RawMessage `json:"payload"`
}

// eventRef — из payload вытаскиваем только ref. Остальные поля (push_id, commits, size,
// distinct_size...) между эпохами различаются (ADR 0007), а нам они здесь не нужны: лишние поля
// в JSON encoding/json молча игнорирует, так что эта структура одинаково разбирает payload
// PushEvent что до, что после смены формата в октябре 2025.
type eventRef struct {
	Ref string `json:"ref"`
}

// ParseGitHubEvent разбирает одно сырое событие в форме, которую отдают и GH Archive, и GitHub
// Events API, и нормализует его к канонической схеме Event. Здесь и только здесь решаются
// особенности проволочного формата GitHub — строковый .id, отсутствующий .org, ref только у части
// типов, — поэтому и archive (задача 1.3), и будущий поллинг Events API (задача 2.9) используют
// один и тот же маппинг, не дублируя его.
//
// Возвращает ошибку на структурно битом или урезанном JSON: невалидный JSON, нечисловой .id,
// отсутствующие .type/.created_at. Строка вида `{"id":"7"}` синтаксически валидна, но без .type
// и .created_at даёт событие с пустым типом и нулевым временем (год 1) — а ClickHouse партиционирует
// по toYYYYMM(created_at), так что такая строка либо создаёт мусорную партицию, либо валит вставку.
// Вызывающий код (internal/archive.decodeLines) уже логирует и пропускает ошибку на уровне строки —
// здесь достаточно просто вернуть error, не изобретая отдельную обработку под "usable but wrong".
func ParseGitHubEvent(data []byte) (Event, error) {
	var raw githubEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return Event{}, fmt.Errorf("decode github event: %w", err)
	}

	if raw.Type == "" {
		return Event{}, fmt.Errorf("event has no type")
	}
	if raw.CreatedAt.IsZero() {
		return Event{}, fmt.Errorf("event has no created_at")
	}

	id, err := strconv.ParseUint(raw.ID, 10, 64)
	if err != nil {
		return Event{}, fmt.Errorf("parse event id %q: %w", raw.ID, err)
	}

	var orgID uint64
	if raw.Org != nil {
		orgID = raw.Org.ID
	}

	payloadLen := payloadByteLen(raw.Payload)

	var ref string
	if refCarryingEventTypes[raw.Type] && payloadLen > 0 {
		var pr eventRef
		// Ошибку разбора payload здесь осознанно проглатываем: если конкретная форма payload
		// вдруг не распарсится (например, ещё одна незадокументированная эпоха), безопаснее
		// оставить ref пустым, чем ронять всё событие целиком ради одного вспомогательного поля.
		_ = json.Unmarshal(raw.Payload, &pr)
		ref = pr.Ref
	}

	return Event{
		EventID:    id,
		EventType:  raw.Type,
		CreatedAt:  raw.CreatedAt,
		ActorID:    raw.Actor.ID,
		ActorLogin: raw.Actor.Login,
		RepoID:     raw.Repo.ID,
		RepoName:   raw.Repo.Name,
		OrgID:      orgID,
		Language:   "",
		// uint32(payloadLen) безопасен только потому, что источник ограничен: строка JSON Lines
		// из GH Archive не превышает 8 МБ (internal/archive.maxLineSize), что на два порядка ниже
		// потолка uint32 (~4.29e9). У Events API (задача 2.9) источник — произвольное тело HTTP-
		// ответа, а не файл с построчным лимитом; эту границу там придётся продумать заново, а не
		// полагаться, что она унаследуется отсюда сама собой.
		PayloadSize: uint32(payloadLen),
		Ref:         ref,
	}, nil
}

// payloadByteLen — длина .payload в байтах для PayloadSize, с одной поправкой: JSON-литерал
// null (валидный, хоть на практике в GH Archive не встречающийся: "payload":null) json.RawMessage
// хранит как строку из четырёх байт "null", а не как отсутствие значения. Событие без payload —
// это 0 байт, а не 4, поэтому null проверяем явно, а не просто берём len(raw).
func payloadByteLen(raw json.RawMessage) int {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	return len(raw)
}
