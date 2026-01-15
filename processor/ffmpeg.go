package processor

import (
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

func CompressWithFFmpeg(inputPath string, logger *logrus.Entry) (string, error) {
	outputPath := inputPath + "_compressed.mp4"
	logger.WithFields(logrus.Fields{
		"input":  inputPath,
		"output": outputPath,
	}).Info("Starting ffmpeg compression (lossless)")

	cmd := exec.Command("ffmpeg", "-i", inputPath, "-c:v", "libx264", "-crf", "0", "-preset", "ultrafast", "-y", outputPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.WithError(err).WithField("ffmpeg_output", string(output)).Error("FFmpeg compression failed")
		return "", err
	}

	logger.Info("FFmpeg compression completed successfully")
	return outputPath, nil
}

// ConvertTSToMP4 faz remux (rápido) de .ts para .mp4, sem re-encode quando possível.
// Retorna o caminho do arquivo .mp4 gerado.
func ConvertTSToMP4(inputPath string, logger *logrus.Entry) (string, error) {
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
		logger.Info("FFmpeg TS->MP4 conversion completed successfully")
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
		logger.WithError(err2).WithField("ffmpeg_output", string(output2)).Error("FFmpeg TS->MP4 conversion failed")
		return "", err2
	}

	logger.Info("FFmpeg TS->MP4 conversion completed successfully")
	return outputPath, nil
}
