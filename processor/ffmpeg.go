package processor

import (
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

func CompressWithFFmpeg(inputPath string, logger *logrus.Entry) (string, error) {
	start := time.Now()
	// Usar extensão .mp4 para que o ffmpeg consiga detectar o formato do muxer corretamente
	outputPath := inputPath + ".compressed.mp4"
	logger.WithFields(logrus.Fields{
		"input":  inputPath,
		"output": outputPath,
	}).Info("Starting ffmpeg compression (standard H.264)")

	// Alterado de CRF 0 (lossless/gigante) para 23 (standard) e preset medium para melhor eficiência
	cmd := exec.Command("ffmpeg", "-i", inputPath, "-c:v", "libx264", "-crf", "23", "-preset", "medium", "-y", outputPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{
			"ffmpeg_output": string(output),
			"duration":      time.Since(start).String(),
		}).Error("FFmpeg compression failed")
		return "", err
	}

	logger.WithField("duration", time.Since(start).String()).Info("FFmpeg compression completed successfully")
	return outputPath, nil
}

// ConvertTSToMP4 faz remux (rápido) de .ts para .mp4, sem re-encode quando possível.
// Retorna o caminho do arquivo .mp4 gerado.
func ConvertTSToMP4(inputPath string, logger *logrus.Entry) (string, error) {
	start := time.Now()
	base := strings.TrimSuffix(inputPath, filepath.Ext(inputPath))
	outputPath := base + ".mp4"

	logger.WithFields(logrus.Fields{
		"input":  inputPath,
		"output": outputPath,
	}).Info("Starting ffmpeg TS->MP4 conversion (remux)")

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
		logger.WithField("duration", time.Since(start).String()).Info("FFmpeg TS->MP4 conversion completed successfully")
		return outputPath, nil
	}
	logger.WithError(err).WithField("ffmpeg_output", string(output)).Warn("FFmpeg TS->MP4 remux failed, retrying with aac_adtstoasc")

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
		logger.WithError(err2).WithFields(logrus.Fields{
			"ffmpeg_output": string(output2),
			"duration":      time.Since(start).String(),
		}).Error("FFmpeg TS->MP4 conversion failed")
		return "", err2
	}

	logger.WithField("duration", time.Since(start).String()).Info("FFmpeg TS->MP4 conversion completed successfully")
	return outputPath, nil
}
