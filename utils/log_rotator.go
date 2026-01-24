package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type DailyRotateWriter struct {
	LogDir    string
	Retention int
	mu        sync.Mutex
	current   *os.File
	lastDate  string
}

func NewDailyRotateWriter(logDir string, retentionDays int) (*DailyRotateWriter, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	return &DailyRotateWriter{
		LogDir:    logDir,
		Retention: retentionDays,
	}, nil
}

func (w *DailyRotateWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	date := time.Now().Format("2006-01-02")
	if date != w.lastDate {
		if err := w.rotate(date); err != nil {
			return 0, err
		}
	}

	return w.current.Write(p)
}

func (w *DailyRotateWriter) rotate(date string) error {
	if w.current != nil {
		w.current.Close()
	}

	filename := filepath.Join(w.LogDir, fmt.Sprintf("%s.log", date))
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.current = f
	w.lastDate = date

	go w.cleanup()

	return nil
}

func (w *DailyRotateWriter) cleanup() {
	w.mu.Lock()
	defer w.mu.Unlock()

	files, err := os.ReadDir(w.LogDir)
	if err != nil {
		return
	}

	var logFiles []string
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".log" {
			logFiles = append(logFiles, f.Name())
		}
	}

	if len(logFiles) <= w.Retention {
		return
	}

	sort.Strings(logFiles)

	// Remove older files until we reach the retention limit
	toRemove := len(logFiles) - w.Retention
	for i := 0; i < toRemove; i++ {
		os.Remove(filepath.Join(w.LogDir, logFiles[i]))
	}
}

func (w *DailyRotateWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current != nil {
		return w.current.Close()
	}
	return nil
}
