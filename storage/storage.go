package storage

import (
"context"
"fmt"
"io"
"mime/multipart"
"os"
"path/filepath"
"strings"

"dvr-upload/config"

"github.com/aws/aws-sdk-go-v2/aws"
s3config "github.com/aws/aws-sdk-go-v2/config"
"github.com/aws/aws-sdk-go-v2/credentials"
"github.com/aws/aws-sdk-go-v2/service/s3"
"github.com/sirupsen/logrus"
)

type StorageService struct {
cfg      *config.Config
s3Client *s3.Client
log      *logrus.Logger
}

func NewStorageService(cfg *config.Config, log *logrus.Logger) *StorageService {
s := &StorageService{
cfg: cfg,
log: log,
}
s.initS3()
return s
}

func (s *StorageService) initS3() {
if s.cfg.S3Bucket == "" || s.cfg.S3Endpoint == "" {
s.log.Warn("S3/OCI storage not fully configured, upload to object storage will be disabled")
return
}

cfg, err := s3config.LoadDefaultConfig(
context.TODO(),
s3config.WithRegion(s.cfg.S3Region),
s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.cfg.S3AccessKey, s.cfg.S3SecretKey, "")),
)
if err != nil {
s.log.WithError(err).Error("Failed to load AWS config for S3-compatible endpoint")
return
}

s.s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
o.BaseEndpoint = aws.String(s.cfg.S3Endpoint)
o.UsePathStyle = s.cfg.S3UsePathStyle
o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
})

s.log.WithFields(logrus.Fields{
"bucket":        s.cfg.S3Bucket,
"endpoint":      s.cfg.S3Endpoint,
"region":        s.cfg.S3Region,
"usePathStyle":  s.cfg.S3UsePathStyle,
"checksum_mode": "when_required",
}).Info("S3-compatible client (AWS SDK v2 / OCI) initialized")
}

func (s *StorageService) SaveUploadedFile(file multipart.File, dstPath string, logger *logrus.Entry) (int64, error) {
if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
logger.WithError(err).WithField("path", dstPath).Error("Failed to create directory for file")
return 0, fmt.Errorf("failed to create directory")
}

dst, err := os.Create(dstPath)
if err != nil {
logger.WithError(err).WithField("path", dstPath).Error("Failed to create destination file")
return 0, fmt.Errorf("failed to save file")
}
defer dst.Close()

if _, err := file.Seek(0, io.SeekStart); err != nil {
logger.WithError(err).Error("Failed to seek to the start of the uploaded file stream")
return 0, fmt.Errorf("failed to read upload stream")
}

logger.WithField("destination_path", dstPath).Info("Starting file write operation")
bytesWritten, err := io.Copy(dst, file)
if err != nil {
os.Remove(dstPath)
logger.WithError(err).WithFields(logrus.Fields{
"path":          dstPath,
"bytes_written": bytesWritten,
}).Error("Failed to write file content, partial file deleted")
return bytesWritten, fmt.Errorf("write failed")
}

logger.WithFields(logrus.Fields{
"path":          dstPath,
"bytes_written": bytesWritten,
}).Info("File written successfully")

return bytesWritten, nil
}

func (s *StorageService) CopyToBackup(srcPath, filename string, logger *logrus.Entry) error {
backupLogger := logger.WithField("backup_process", true)
backupLogger.Infof("Starting backup for %s", filename)

srcFile, err := os.Open(srcPath)
if err != nil {
return fmt.Errorf("failed to open source file: %w", err)
}
defer srcFile.Close()

backupDestPath := filepath.Join(s.cfg.BackupPath, filename)
if err := os.MkdirAll(filepath.Dir(backupDestPath), 0755); err != nil {
return fmt.Errorf("failed to create backup directory: %w", err)
}

dstFile, err := os.Create(backupDestPath)
if err != nil {
return fmt.Errorf("failed to create backup file: %w", err)
}
defer dstFile.Close()

bytesCopied, err := io.Copy(dstFile, srcFile)
if err != nil {
os.Remove(backupDestPath)
return fmt.Errorf("failed to copy file content to backup (copied %d bytes): %w", bytesCopied, err)
}

backupLogger.WithFields(logrus.Fields{
"source_path":  srcPath,
"backup_path":  backupDestPath,
"bytes_copied": bytesCopied,
}).Infof("Successfully backed up %s", filename)
return nil
}

func (s *StorageService) UploadFileToS3(filePath string, filename string, logger *logrus.Entry) error {
if s.s3Client == nil || s.cfg.S3Bucket == "" {
logger.Warn("S3 upload skipped: client not initialized or bucket not set")
return nil
}

f, err := os.Open(filePath)
if err != nil {
return fmt.Errorf("failed to open file: %w", err)
}
defer f.Close()

stat, err := f.Stat()
if err != nil {
return fmt.Errorf("failed to stat file: %w", err)
}

contentType := "application/octet-stream"
switch strings.ToLower(filepath.Ext(filename)) {
case ".mp4":
contentType = "video/mp4"
case ".ts":
contentType = "video/MP2T"
case ".jpg", ".jpeg":
contentType = "image/jpeg"
}

logger.WithFields(logrus.Fields{
"bucket":       s.cfg.S3Bucket,
"key":          filename,
"filepath":     filePath,
"size":         stat.Size(),
"content_type": contentType,
}).Info("Uploading file to S3 (AWS SDK v2 / OCI)")

_, err = s.s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
Bucket:        aws.String(s.cfg.S3Bucket),
Key:           aws.String(filename),
Body:          f,
ContentLength: aws.Int64(stat.Size()),
ContentType:   aws.String(contentType),
})
if err != nil {
return fmt.Errorf("failed to upload to S3-compatible storage: %w", err)
}

logger.Info("S3-compatible upload successful")
return nil
}
