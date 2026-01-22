package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"dvr-upload/config"
	"dvr-upload/handlers"
	"dvr-upload/queue"
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

	rabbitMQ, err := queue.NewRabbitMQClient(cfg.RabbitMQURL, cfg.RabbitMQQueue, cfg.RabbitMQExchange, cfg.RabbitMQTtl, log)
	if err != nil {
		log.WithError(err).Warn("Failed to initialize RabbitMQ client")
	} else {
		defer rabbitMQ.Close()
	}

	h := handlers.NewHandler(cfg, storageService, rabbitMQ, log)

	mux := http.NewServeMux()
	mux.HandleFunc("/upload", h.UploadHandler)
	mux.HandleFunc("/health", h.HealthHandler)

	srv := &http.Server{
		Addr:              ":23010",
		Handler:           mux,
		ReadTimeout:       0, // Permite uploads longos
		WriteTimeout:      0, // Permite processamento longo
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 30 * time.Second, // Timeout para cabeçalhos (evita Slowloris parcial)
		MaxHeaderBytes:    1 << 20,          // 1MB
	}

	log.WithField("addr", srv.Addr).Info("Starting HTTP server")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %s", err)
	}
}
