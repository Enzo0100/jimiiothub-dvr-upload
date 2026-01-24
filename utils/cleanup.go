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
	// Aumentado para 5 minutos para ser mais responsivo com arquivos interrompidos
	ticker := time.NewTicker(5 * time.Minute)
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
	processingDirs := []string{}

	addDirs := func(basePath string) {
		if basePath == "" {
			return
		}
		dirs = append(dirs, basePath)
		// Pasta antiga (dentro)
		pDirOld := filepath.Join(basePath, ".processing")
		dirs = append(dirs, pDirOld)
		processingDirs = append(processingDirs, pDirOld)
		// Pasta nova (fora/irmã)
		pDirNew := filepath.Join(filepath.Dir(filepath.Clean(basePath)), ".processing_"+filepath.Base(filepath.Clean(basePath)))
		dirs = append(dirs, pDirNew)
		processingDirs = append(processingDirs, pDirNew)
	}

	addDirs(videoPath)
	if backupPath != "" && backupPath != videoPath {
		addDirs(backupPath)
	}

	// Aumentado para 30 minutos - tempo suficiente para uploads lentos mas limpa rápido os mortos
	maxAge := 5 * time.Minute
	now := time.Now()
	cleanedCount := 0

	for _, dir := range dirs {
		if dir == "" {
			continue
		}

		// Verifica se este diretório é uma pasta .processing
		isProcessingDir := false
		for _, pd := range processingDirs {
			if dir == pd {
				isProcessingDir = true
				break
			}
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
			info, err := entry.Info()
			if err != nil {
				continue
			}

			// Lógica de decisão de remoção baseada na solicitação:
			// Nunca apagar vídeos/imagens a menos que sejam temporários ou não tenham nome.
			ext := strings.ToLower(filepath.Ext(name))
			isMedia := ext == ".mp4" || ext == ".ts" || ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".mkv" || ext == ".avi"
			isTemp := strings.HasPrefix(name, "upload-stream-") ||
				strings.HasSuffix(name, ".tmp") ||
				strings.Contains(name, ".compressed")

			// Consideramos "sem nome" se o nome base (sem extensão) for vazio (ex: ".mp4", ".ts")
			hasNoName := strings.TrimSuffix(name, ext) == ""

			shouldRemove := false
			if now.Sub(info.ModTime()) > maxAge {
				if isProcessingDir {
					// No diretório .processing, remove se (antigo) E (não é mídia OU é temp OU sem nome)
					if !isMedia || isTemp || hasNoName {
						shouldRemove = true
					}
				} else {
					// Em pastas comuns, remove se (antigo) E (é temp OU sem nome OU não for mídia)
					// Isso protege arquivos de vídeo/imagem legítimos que caíram na raiz por engano
					if isTemp || hasNoName || !isMedia {
						shouldRemove = true
					}
				}
			}

			if shouldRemove {
				path := filepath.Join(dir, name)
				if err := os.Remove(path); err == nil {
					cleanedCount++
				}
			}
		}
	}

	if cleanedCount > 0 {
		logger.Info("Periodic cleanup finished", "files_removed", cleanedCount)
	}
}
