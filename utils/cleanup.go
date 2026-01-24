package utils

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// StartCleanupTask inicia uma goroutine que limpa periodicamente arquivos temporários órfãos.
func StartCleanupTask(videoPath string, backupPath string, logger *slog.Logger) {
	// Aumentado para 15 minutos para ser menos agressivo
	ticker := time.NewTicker(15 * time.Minute)
	go func() {
		// Executa uma vez no início
		cleanup(videoPath, backupPath, logger)
		for range ticker.C {
			cleanup(videoPath, backupPath, logger)
		}
	}()
}

func cleanup(videoPath string, backupPath string, logger *slog.Logger) {
	dirs := []string{os.TempDir()}
	if videoPath != "" {
		dirs = append(dirs, videoPath)
		dirs = append(dirs, filepath.Join(videoPath, ".processing"))
	}
	if backupPath != "" && backupPath != videoPath {
		dirs = append(dirs, backupPath)
		dirs = append(dirs, filepath.Join(backupPath, ".processing"))
	}

	// Também verifica o diretório atual do processo
	if pwd, err := os.Getwd(); err == nil {
		dirs = append(dirs, pwd)
	}

	// Aumentado para 4 horas para evitar deletar arquivos em processamento ou por timezone
	maxAge := 4 * time.Hour
	now := time.Now()
	cleanedCount := 0

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			name := entry.Name()
			// Padrões de arquivos temporários que podem ficar órfãos
			isTemp := strings.HasPrefix(name, "upload-stream-") ||
				strings.HasSuffix(name, ".tmp") ||
				strings.Contains(name, ".compressed.mp4")

			if isTemp {
				info, err := entry.Info()
				if err != nil {
					continue
				}

				// Só deleta se for realmente antigo
				modTime := info.ModTime()
				if now.Sub(modTime) > maxAge {
					path := filepath.Join(dir, name)
					if err := os.Remove(path); err == nil {
						cleanedCount++
					}
				}
			}
		}
	}

	if cleanedCount > 0 {
		logger.Info("Periodic cleanup finished", "files_removed", cleanedCount)
	}
}
