package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"

	"dvr-upload/config"
	"dvr-upload/handlers"
	"dvr-upload/storage"

	"github.com/sirupsen/logrus"
)

var (
	log = logrus.New()
)

func main() {
	cfg := config.LoadConfig()

	// setup logging
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.InfoLevel)
	if err := os.MkdirAll(filepath.Dir(cfg.LogFilePath), 0755); err == nil {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			// log to file and console
			mw := io.MultiWriter(os.Stdout, f)
			log.SetOutput(mw)
		}
	}

	log.WithFields(logrus.Fields{
		"listen_addr": ":23010",
		"video_path":  cfg.VideoPath,
		"backup_path": cfg.BackupPath,
		"dr_mode":     cfg.DisasterRecoveryMode,
	}).Info("[UploadServer] Starting server")

	if cfg.EnableLocalStorage {
		os.MkdirAll(cfg.VideoPath, 0755)
	}

	storageService := storage.NewStorageService(cfg, log)
	h := handlers.NewHandler(cfg, storageService, log)

	http.HandleFunc("/upload", h.UploadHandler)
	http.HandleFunc("/ping", h.PingHandler)
	log.Fatal(http.ListenAndServe(":23010", nil))
}
