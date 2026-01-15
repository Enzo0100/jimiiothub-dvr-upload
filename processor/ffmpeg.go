package processor

import (
"os/exec"
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
