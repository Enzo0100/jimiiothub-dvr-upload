package main

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"dvr-upload/config"
	"dvr-upload/handlers"
	"dvr-upload/queue"
	"dvr-upload/storage"
	"dvr-upload/utils"
)

var (
	logger *slog.Logger
)

func main() {
	cfg := config.LoadConfig()

	// setup logging
	var mw io.Writer = os.Stdout
	logDir := filepath.Dir(cfg.LogFilePath)
	if rotator, err := utils.NewDailyRotateWriter(logDir, 7); err == nil {
		mw = io.MultiWriter(os.Stdout, rotator)
		defer rotator.Close()
	} else {
		slog.Error("Failed to initialize log rotator", "error", err, "dir", logDir)
	}

	handler := slog.NewJSONHandler(mw, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger = slog.New(handler)
	slog.SetDefault(logger)

	logger.Info("[UploadServer] Starting server",
		"listen_addr", ":23010",
		"video_path", cfg.VideoPath,
		"backup_path", cfg.BackupPath,
		"dr_mode", cfg.DisasterRecoveryMode)

	if cfg.EnableLocalStorage {
		os.MkdirAll(cfg.VideoPath, 0755)
	}

	storageService := storage.NewStorageService(cfg, logger)

	rabbitMQ, err := queue.NewRabbitMQClient(cfg.RabbitMQURL, cfg.RabbitMQQueue, cfg.RabbitMQExchange, cfg.RabbitMQTtl, logger)
	if err != nil {
		logger.Warn("Failed to initialize RabbitMQ client", "error", err)
	} else {
		defer rabbitMQ.Close()
	}

	h := handlers.NewHandler(cfg, storageService, rabbitMQ, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("/upload", h.UploadHandler)
	mux.HandleFunc("/health", h.HealthHandler)
	mux.HandleFunc("/test", h.TestPageHandler)

	srv := &http.Server{
		Addr:              ":23010",
		Handler:           mux,
		ReadTimeout:       0, // Permite uploads longos
		WriteTimeout:      0, // Permite processamento longo
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 30 * time.Second, // Timeout para cabeçalhos (evita Slowloris parcial)
		MaxHeaderBytes:    1 << 20,          // 1MB
	}

	logger.Info("Starting HTTP server", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Server failed", "error", err)
		os.Exit(1)
	}
}
