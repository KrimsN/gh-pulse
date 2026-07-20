// Package logfile — io.WriteCloser поверх os.File с простой ротацией по размеру (задача 4.4).
//
// Не lumberjack и не другая сторонняя зависимость: это dev/demo-инструмент (файл читает `/admin/logs`
// в pulse-api через bind mount `./logs`, см. docker-compose.yml), не production log pipeline —
// заводить ELK/Loki-уровневую ротацию (несколько поколений, сжатие, время жизни) незачем. Одна
// резервная копия (`.1`) достаточна, чтобы файл не рос неограниченно за демо-сессию.
package logfile

import (
	"fmt"
	"os"
	"sync"
)

// DefaultMaxBytes — порог ротации по умолчанию.
const DefaultMaxBytes = 10 * 1024 * 1024

// Writer пишет в файл по пути Path, переименовывая его в Path+".1" и открывая заново, как только
// очередная запись превысила бы MaxBytes. Комментарий-докстрока полей вместо конструктора с опциями:
// единственный вызывающий (cmd/gh-collector/main.go) создаёt Writer один раз при старте, второй
// конфигурации не появится — options pattern был бы абстракцией без второго случая использования.
type Writer struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	file     *os.File
	size     int64
}

// New открывает (или создаёт) файл по path в режиме дозаписи и готовит Writer к ротации на maxBytes.
func New(path string, maxBytes int64) (*Writer, error) {
	file, size, err := openAppend(path)
	if err != nil {
		return nil, err
	}
	return &Writer{path: path, maxBytes: maxBytes, file: file, size: size}, nil
}

func openAppend(path string) (*os.File, int64, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("open log file %s: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, fmt.Errorf("stat log file %s: %w", path, err)
	}
	return file, info.Size(), nil
}

// Write реализует io.Writer. Ротация проверяется перед записью, а не после: последняя запись перед
// превышением порога остаётся в старом файле целиком, а не разрезанной между старым и новым.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size+int64(len(p)) > w.maxBytes {
		if err := w.rotate(); err != nil {
			// Ошибка ротации не должна ронять запись целиком — пишем в уже открытый (пусть и
			// переполненный) файл, чем теряем строку лога молча.
			fmt.Fprintf(os.Stderr, "logfile: rotate %s: %v\n", w.path, err)
		}
	}

	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *Writer) rotate() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close before rotation: %w", err)
	}

	backupPath := w.path + ".1"
	if err := os.Rename(w.path, backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rename to backup: %w", err)
	}

	file, size, err := openAppend(w.path)
	if err != nil {
		return fmt.Errorf("reopen after rotation: %w", err)
	}
	w.file = file
	w.size = size
	return nil
}

// Close закрывает файл. Вызывающий (main.go) обязан вызвать её через defer после New — иначе
// последние буферизованные ОС записи рискуют не долететь до диска при аварийном завершении процесса.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
