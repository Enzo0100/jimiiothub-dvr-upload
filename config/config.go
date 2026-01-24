package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	SecretKey            string
	EnableSecret         bool
	VideoPath            string
	BackupPath           string
	DisasterRecoveryMode bool
	EnableLocalStorage   bool
	EnableTsToMp4        bool
	EnableS3Upload       bool
	EnableRabbitMQ       bool
	LogFilePath          string

	// S3 Configuration (OCI Compatibility)
	S3Bucket       string
	S3Region       string
	S3Endpoint     string
	S3AccessKey    string
	S3SecretKey    string
	S3UsePathStyle bool

	// RabbitMQ Configuration
	RabbitMQURL      string
	RabbitMQQueue    string
	RabbitMQExchange string
	RabbitMQTtl      int

	// Workers Configuration
	MaxConcurrentWorkers int
	EnableCompression    bool
}

func LoadConfig() *Config {
	// Construct RabbitMQ URL from individual components
	rmqHost := getEnv("RABBITMQ_HOST", "localhost")
	rmqPort := getEnv("RABBITMQ_PORT", "5672")
	rmqUser := getEnv("RABBITMQ_USER", "guest")
	rmqPass := getEnv("RABBITMQ_PASSWORD", "guest")

	rmqURL := fmt.Sprintf("amqp://%s:%s@%s:%s/", rmqUser, rmqPass, rmqHost, rmqPort)

	return &Config{
		SecretKey:            getEnv("SECRET_KEY", "jimidvr@123!443"),
		EnableSecret:         getEnv("ENABLE_SECRET", "true") == "true",
		VideoPath:            getEnv("LOCAL_VIDEO_PATH", "/data/upload"),
		BackupPath:           getEnv("BACKUP_VIDEO_PATH", "/data/dvr-upload-backup"),
		DisasterRecoveryMode: getEnv("DISASTER_RECOVERY_MODE", "false") == "true",
		EnableLocalStorage:   getEnv("ENABLE_LOCAL_STORAGE", "true") == "true",
		EnableTsToMp4:        getEnv("ENABLE_TS_TO_MP4", "true") == "true",
		EnableS3Upload:       getEnv("ENABLE_S3_UPLOAD", "true") == "true",
		EnableRabbitMQ:       getEnv("ENABLE_RABBITMQ", "true") == "true",
		LogFilePath:          "/app/dvr-upload/logs/server.log",

		S3Bucket:       getEnv("OCI_BUCKET_MEDIA", ""),
		S3Region:       getEnv("OCI_REGION", "sa-saopaulo-1"),
		S3Endpoint:     getEnv("OCI_ENDPOINT", ""),
		S3AccessKey:    getEnv("OCI_ACCESS_KEY_ID", ""),
		S3SecretKey:    getEnv("OCI_SECRET_ACCESS_KEY", ""),
		S3UsePathStyle: strings.EqualFold(getEnv("OCI_USE_PATH_STYLE_ENDPOINT", "true"), "true"),

		RabbitMQURL:      rmqURL,
		RabbitMQQueue:    getEnv("RABBITMQ_QUEUE", "dvr_upload_events"),
		RabbitMQExchange: getEnv("RABBITMQ_EXCHANGE", "iothub-webhook"),
		RabbitMQTtl:      getEnvAsInt("RABBITMQ_TTL", 300000),

		MaxConcurrentWorkers: getEnvAsInt("MAX_CONCURRENT_WORKERS", 6),
		EnableCompression:    getEnv("ENABLE_COMPRESSION", "true") == "true",
	}
}

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

func getEnvAsInt(key string, def int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return def
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return def
	}
	return val
}
