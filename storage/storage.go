package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dvr-upload/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type StorageService struct {
	cfg      *config.Config
	s3Client *s3.Client
	log      *slog.Logger
}

func NewStorageService(cfg *config.Config, log *slog.Logger) *StorageService {
	s := &StorageService{
		cfg: cfg,
		log: log,
	}
	s.initS3()
	return s
}

func (s *StorageService) initS3() {
	if !s.cfg.EnableS3Upload {
		return
	}

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
		s.log.Error("Failed to load AWS config for S3-compatible endpoint", "error", err)
		return
	}

	s.s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s.cfg.S3Endpoint)
		o.UsePathStyle = s.cfg.S3UsePathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	s.log.Info("S3-compatible client (AWS SDK v2 / OCI) initialized",
		"bucket", s.cfg.S3Bucket,
		"endpoint", s.cfg.S3Endpoint,
		"region", s.cfg.S3Region,
		"use_path_style", s.cfg.S3UsePathStyle,
		"checksum_mode", "when_required")
}

func (s *StorageService) Ping() error {
	if s.s3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}
	_, err := s.s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	return err
}

func (s *StorageService) SaveUploadedFile(file multipart.File, dstPath string, logger *slog.Logger) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
		logger.Error("Failed to create directory for file", "path", dstPath, "error", err)
		return 0, fmt.Errorf("failed to create directory")
	}

	dst, err := os.Create(dstPath)
	if err != nil {
		logger.Error("Failed to create destination file", "path", dstPath, "error", err)
		return 0, fmt.Errorf("failed to save file")
	}
	defer dst.Close()

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		logger.Error("Failed to seek to the start of the uploaded file stream", "error", err)
		return 0, fmt.Errorf("failed to read upload stream")
	}

	logger.Info("Starting file write operation", "destination_path", dstPath)
	bytesWritten, err := io.Copy(dst, file)
	if err != nil {
		os.Remove(dstPath)
		logger.Error("Failed to write file content, partial file deleted",
			"path", dstPath,
			"bytes_written", bytesWritten,
			"error", err)
		return bytesWritten, fmt.Errorf("write failed")
	}

	logger.Info("File written successfully",
		"path", dstPath,
		"bytes_written", bytesWritten)

	return bytesWritten, nil
}

func (s *StorageService) CopyToBackup(srcPath, filename string, logger *slog.Logger) error {
	backupLogger := logger.With("backup_process", true)
	backupLogger.Info("Starting backup", "filename", filename)

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

	backupLogger.Info("Successfully backed up",
		"filename", filename,
		"source_path", srcPath,
		"backup_path", backupDestPath,
		"bytes_copied", bytesCopied)
	return nil
}

func (s *StorageService) UploadFileToS3(filePath string, filename string, logger *slog.Logger) error {
	if s.s3Client == nil || s.cfg.S3Bucket == "" {
		logger.Warn("S3 upload skipped: client not initialized or bucket not set")
		return nil
	}

	start := time.Now()
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := stat.Size()

	contentType := "application/octet-stream"
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".mp4":
		contentType = "video/mp4"
	case ".ts":
		contentType = "video/MP2T"
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.cfg.S3Bucket),
		Key:           aws.String(filename),
		Body:          f,
		ContentLength: aws.Int64(fileSize),
		ContentType:   aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3-compatible storage (duration: %s): %w", time.Since(start), err)
	}

	return nil
}
