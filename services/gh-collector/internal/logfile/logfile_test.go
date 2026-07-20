package logfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriter_AppendsWithoutRotationUnderThreshold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	w, err := New(path, 1024)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = w.Close() }()

	if _, err := w.Write([]byte("line one\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := w.Write([]byte("line two\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if got, want := string(data), "line one\nline two\n"; got != want {
		t.Fatalf("file content = %q, want %q", got, want)
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatalf("backup file should not exist under threshold, stat err = %v", err)
	}
}

func TestWriter_RotatesWhenExceedingMaxBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	// maxBytes достаточно мал, чтобы вторая запись сама по себе его превысила.
	w, err := New(path, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = w.Close() }()

	first := []byte("0123456789")
	if _, err := w.Write(first); err != nil {
		t.Fatalf("Write first: %v", err)
	}
	second := []byte("abcdefghij")
	if _, err := w.Write(second); err != nil {
		t.Fatalf("Write second: %v", err)
	}

	backup, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatalf("ReadFile backup: %v", err)
	}
	if string(backup) != string(first) {
		t.Fatalf("backup content = %q, want %q", backup, first)
	}

	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile current: %v", err)
	}
	if string(current) != string(second) {
		t.Fatalf("current content = %q, want %q", current, second)
	}
}

func TestWriter_ResumesExistingFileSizeOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	if err := os.WriteFile(path, []byte("preexisting"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	w, err := New(path, 15)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = w.Close() }()

	// "preexisting" (11 байт) + "more" (4 байта) = 15, не больше maxBytes — ротации быть не должно.
	if _, err := w.Write([]byte("more")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if got, want := string(data), "preexistingmore"; got != want {
		t.Fatalf("file content = %q, want %q", got, want)
	}
}
