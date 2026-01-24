package processor

import (
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func CompressWithFFmpeg(inputPath string, logger *slog.Logger) (string, error) {
	// Verificar se o arquivo tem stream de vídeo antes de tentar comprimir
	probeCmd := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_type", "-of", "csv=p=0", inputPath)
	probeOutput, err := probeCmd.Output()
	if err != nil || strings.TrimSpace(string(probeOutput)) == "" {
		// Se não tem vídeo ou erro no ffprobe, ignora compressão e não retorna erro (prosseguirá com original)
		logger.Info("File does not contain a video stream or ffprobe failed, skipping compression", "path", inputPath)
		return "", nil // Retornar vazio sem erro faz o handler usar o original
	}

	start := time.Now()
	// Usar extensão .mp4 para que o ffmpeg consiga detectar o formato do muxer corretamente
	outputPath := inputPath + ".compressed.mp4"

	// Setup otimizado para máxima eficiência de compressão (tamanho vs qualidade):
	// Preset 'ultrafast' para reduzir tempo de CPU ao máximo em troca de arquivos um pouco maiores
	// CRF 30 oferece uma compressão excelente (arquivos bem pequenos) com qualidade aceitável para DVR.
	// -movflags +faststart permite que o vídeo comece a tocar antes de baixar todo o arquivo.
	cmd := exec.Command("ffmpeg", "-i", inputPath,
		"-c:v", "libx264",
		"-crf", "30",
		"-preset", "ultrafast",
		"-threads", "1",
		"-movflags", "+faststart",
		"-pix_fmt", "yuv420p", // Garante compatibilidade máxima com browsers/players
		"-y", outputPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("FFmpeg compression failed",
			"error", err,
			"ffmpeg_output", string(output),
			"duration", time.Since(start).String())
		return "", err
	}

	return outputPath, nil
}

// ConvertTSToMP4 faz remux (rápido) de .ts para .mp4, sem re-encode quando possível.
// Retorna o caminho do arquivo .mp4 gerado.
func ConvertTSToMP4(inputPath string, logger *slog.Logger) (string, error) {
	start := time.Now()
	base := strings.TrimSuffix(inputPath, filepath.Ext(inputPath))
	outputPath := base + ".mp4"

	// Tentativa 1: remux simples (mais compatível).
	cmd := exec.Command(
		"ffmpeg",
		"-y",
		"-i", inputPath,
		"-map", "0",
		"-c", "copy",
		"-movflags", "+faststart",
		outputPath,
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return outputPath, nil
	}
	logger.Warn("FFmpeg TS->MP4 remux failed, retrying with aac_adtstoasc", "error", err, "ffmpeg_output", string(output))

	// Tentativa 2: com bitstream filter de AAC (quando TS contém AAC em ADTS).
	cmd2 := exec.Command(
		"ffmpeg",
		"-y",
		"-i", inputPath,
		"-map", "0",
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		"-movflags", "+faststart",
		outputPath,
	)
	output2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		logger.Error("FFmpeg TS->MP4 conversion failed",
			"error", err2,
			"ffmpeg_output", string(output2),
			"duration", time.Since(start).String())
		return "", err2
	}

	return outputPath, nil
}
