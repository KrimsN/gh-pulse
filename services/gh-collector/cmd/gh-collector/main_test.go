package main

import (
	"testing"
	"time"
)

// TestParseHour — табличный тест на разбор --hour. Отдельно фиксирует случаи, из-за которых
// прежняя реализация на fmt.Sscanf + time.Date молча качала не тот час вместо ошибки:
// нераспознанный хвост, месяц/день/час вне диапазона — все обязаны стать ошибкой, а не тихой
// нормализацией на другую дату.
func TestParseHour(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "обычный час с двузначным часом",
			input: "2026-06-01-15",
			want:  time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC),
		},
		{
			name:  "час без ведущего нуля",
			input: "2026-06-01-9",
			want:  time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC),
		},
		{
			name:  "час 0 — полночь",
			input: "2026-01-05-0",
			want:  time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "час 23 — последний час суток, граница",
			input: "2026-01-05-23",
			want:  time.Date(2026, 1, 5, 23, 0, 0, 0, time.UTC),
		},
		{
			name:    "хвост после часа — раньше проглатывался Sscanf",
			input:   "2026-06-01-15-garbage",
			wantErr: true,
		},
		{
			name:    "месяц и день вне диапазона — раньше time.Date молча уезжал на 2027-02-18",
			input:   "2026-13-45-99",
			wantErr: true,
		},
		{
			name:    "час 24 — раньше молча переносился на следующий день, час 0",
			input:   "2026-06-01-24",
			wantErr: true,
		},
		{
			name:    "месяц и день 0 — раньше молча уезжал на 2025-11-30",
			input:   "2026-00-00-0",
			wantErr: true,
		},
		{
			name:    "отрицательный час",
			input:   "2026-06-01--1",
			wantErr: true,
		},
		{
			name:    "меньше четырёх полей",
			input:   "2026-06-01",
			wantErr: true,
		},
		{
			name:    "час не число",
			input:   "2026-06-01-abc",
			wantErr: true,
		},
		{
			name:    "пустая строка",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHour(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("ожидалась ошибка, получено %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("parseHour(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
