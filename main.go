package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type JSONResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

const maxFilenameLength = 255

var (
	log                  = logrus.New()
	secretKey            = getEnv("SECRET_KEY", "jimidvr@123!443")
	enableSecret         = getEnv("ENABLE_SECRET", "true") == "true"
	videoPath            = getEnv("LOCAL_VIDEO_PATH", "/data/upload")
	backupPath           = getEnv("BACKUP_VIDEO_PATH", "/data/dvr-upload-backup")
	disasterRecoveryMode = getEnv("DISASTER_RECOVERY_MODE", "false") == "true"
	logFilePath          = "/app/dvr-upload/logs/server.log"
)

func main() {
	// setup logging
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.InfoLevel)
	if err := os.MkdirAll(filepath.Dir(logFilePath), 0755); err == nil {
		f, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			// log to file and console
			mw := io.MultiWriter(os.Stdout, f)
			log.SetOutput(mw)
		}
	}

	log.WithFields(logrus.Fields{
		"listen_addr": ":23010",
		"video_path":  videoPath,
		"backup_path": backupPath,
		"dr_mode":     disasterRecoveryMode,
	}).Info("[UploadServer] Starting server")

	os.MkdirAll(videoPath, 0755)

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/ping", pingHandler)
	log.Fatal(http.ListenAndServe(":23010", nil))
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()
	logger := log.WithFields(logrus.Fields{
		"request_id":  requestID,
		"remote_addr": r.RemoteAddr,
		"method":      r.Method,
		"uri":         r.RequestURI,
	})

	logger.Info("Upload request received")

	if err := r.ParseMultipartForm(64 << 20); err != nil {
		logger.WithError(err).Error("Failed to parse multipart form")
		writeJSON(w, http.StatusBadRequest, JSONResponse{Code: 400, Message: "Invalid form data"})
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		logger.WithError(err).Error("File is required in the form")
		writeJSON(w, http.StatusBadRequest, JSONResponse{Code: 400, Message: "File is required"})
		return
	}
	defer file.Close()

	fileSize := handler.Size
	logger = logger.WithField("original_filesize", fileSize)

	// Original provided (may be empty / not trusted)
	providedFilename := r.FormValue("filename")
	timestamp := r.FormValue("timestamp")
	sign := r.FormValue("sign")

	// Attempt to build standardized filename if enough parameters are present
	// Pattern desired (event): EVENT_{imei}_00000000_YYYY_MM_DD_HH_mm_SS_{I|F}_{channel}.{ext}
	// For images (snapshot) we keep client provided name unless we can parse fields
	builtName, buildErr := buildStandardFilename(r, handler)
	// Prefer the client-provided filename if present (to match signature and device expectations)
	finalFilename := strings.TrimSpace(providedFilename)
	if finalFilename == "" {
		if buildErr == nil && builtName != "" {
			finalFilename = builtName
		} else {
			finalFilename = handler.Filename
		}
	}
	// sanitize path
	finalFilename = filepath.Base(finalFilename)

	reqLogger := logger.WithFields(logrus.Fields{
		"original_filename": handler.Filename,
		"final_filename":    finalFilename,
		"provided_filename": providedFilename,
		"timestamp":         timestamp,
		"build_error":       buildErr,
	})
	reqLogger.Info("Processing file upload")

	if len(finalFilename) > maxFilenameLength {
		reqLogger.Error("Final filename exceeds max length")
		writeJSON(w, http.StatusInternalServerError, JSONResponse{Code: 500, Message: "File name too long"})
		return
	}

	if enableSecret {
		// Compute signature using the same string the client used.
		baseForSign := providedFilename
		if strings.TrimSpace(baseForSign) == "" {
			baseForSign = finalFilename
		}
		expected := generateSign(baseForSign, timestamp, secretKey)
		if sign != expected {
			reqLogger.WithFields(logrus.Fields{
				"received_sign": sign,
				"expected_sign": expected,
				"base_for_sign": baseForSign,
			}).Warn("Invalid signature")
			writeJSON(w, http.StatusBadRequest, JSONResponse{Code: 400, Message: "Signature error"})
			return
		}
		reqLogger.Info("Signature validated successfully")
	}

	var bytesWritten int64
	if disasterRecoveryMode {
		// Modo de recuperação de desastres: salvar diretamente no backup
		if backupPath == "" {
			reqLogger.Error("Disaster Recovery mode is ON but BACKUP_VIDEO_PATH is not set.")
			writeJSON(w, http.StatusInternalServerError, JSONResponse{Code: 500, Message: "Disaster recovery misconfigured"})
			return
		}
		backupDestPath := filepath.Join(backupPath, finalFilename)
		bytesWritten, err = saveUploadedFile(file, backupDestPath, reqLogger)
		if err != nil {
			// Error is already logged in saveUploadedFile
			writeJSON(w, http.StatusInternalServerError, JSONResponse{Code: 500, Message: err.Error()})
			return
		}
		reqLogger.WithFields(logrus.Fields{
			"path":          backupDestPath,
			"bytes_written": bytesWritten,
		}).Info("Upload success (DR mode)")
	} else {
		// Modo normal: salvar no caminho principal e depois copiar para o backup
		dstPath := filepath.Join(videoPath, finalFilename)
		bytesWritten, err = saveUploadedFile(file, dstPath, reqLogger)
		if err != nil {
			// Error is already logged in saveUploadedFile
			writeJSON(w, http.StatusInternalServerError, JSONResponse{Code: 500, Message: err.Error()})
			return
		}

		// Se o backup estiver habilitado, copie o arquivo em segundo plano
		if backupPath != "" {
			go func(srcPath, dstFilename string, parentLogger *logrus.Entry) {
				if err := copyToBackup(srcPath, dstFilename, parentLogger); err != nil {
					parentLogger.WithError(err).Errorf("Failed to backup %s", dstFilename)
				}
			}(dstPath, finalFilename, reqLogger)
		}
		reqLogger.WithFields(logrus.Fields{
			"path":          dstPath,
			"bytes_written": bytesWritten,
		}).Info("Upload success")
	}

	if fileSize != bytesWritten {
		reqLogger.Warnf("File size mismatch. Original: %d, Written: %d. Possible incomplete upload.", fileSize, bytesWritten)
	}

	writeJSON(w, http.StatusOK, JSONResponse{Code: 200, Message: "File upload success", Data: finalFilename})
}

// saveUploadedFile cria e salva o conteúdo de um multipart.File em um caminho de destino.
func saveUploadedFile(file multipart.File, dstPath string, logger *logrus.Entry) (int64, error) {
	// Certifique-se de que o diretório de destino exista
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

	// Volte ao início do arquivo de upload para garantir que ele possa ser lido
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		logger.WithError(err).Error("Failed to seek to the start of the uploaded file stream")
		return 0, fmt.Errorf("failed to read upload stream")
	}

	logger.WithField("destination_path", dstPath).Info("Starting file write operation")
	bytesWritten, err := io.Copy(dst, file)
	if err != nil {
		// Tenta remover o arquivo parcial em caso de erro de escrita
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

func copyToBackup(srcPath, filename string, logger *logrus.Entry) error {
	backupLogger := logger.WithField("backup_process", true)
	backupLogger.Infof("Starting backup for %s", filename)

	// 1. Abrir o arquivo de origem
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	// 2. Criar a estrutura de diretórios do backup
	backupDestPath := filepath.Join(backupPath, filename)
	if err := os.MkdirAll(filepath.Dir(backupDestPath), 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// 3. Criar o arquivo de destino no backup
	dstFile, err := os.Create(backupDestPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer dstFile.Close()

	// 4. Copiar o conteúdo
	bytesCopied, err := io.Copy(dstFile, srcFile)
	if err != nil {
		os.Remove(backupDestPath) // Remove partial backup file
		return fmt.Errorf("failed to copy file content to backup (copied %d bytes): %w", bytesCopied, err)
	}

	backupLogger.WithFields(logrus.Fields{
		"source_path":  srcPath,
		"backup_path":  backupDestPath,
		"bytes_copied": bytesCopied,
	}).Infof("Successfully backed up %s", filename)
	return nil
}

func generateSign(filename, timestamp, secret string) string {
	sum := md5.Sum([]byte(filename + timestamp + secret))
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%x", sum)))
}

func writeJSON(w http.ResponseWriter, status int, resp JSONResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, JSONResponse{Code: 200, Message: "ok"})
}

// buildStandardFilename tries to create a standardized filename for DVR uploads based
// on form parameters. It supports two patterns:
//  1. Event video / segment: EVENT_{imei}_00000000_YYYY_MM_DD_HH_mm_SS_{I|F}_{channel}.{ext}
//     Required fields: imei, datetime (RFC3339, unix seconds, or YYYYMMDDHHmmSS), type (I/F), channel
//  2. Snapshot image: {imei}_{rawhex}_{channel}_{index}.{ext}  (if fields imei, rawhex, channel, index provided)
//
// If insufficient data, returns empty string and an error.
func buildStandardFilename(r *http.Request, fh *multipart.FileHeader) (string, error) {
	imei := strings.TrimSpace(r.FormValue("imei"))
	typ := strings.TrimSpace(strings.ToUpper(r.FormValue("type"))) // I or F
	channel := strings.TrimSpace(r.FormValue("channel"))
	dtRaw := strings.TrimSpace(r.FormValue("datetime"))
	pattern := strings.TrimSpace(strings.ToLower(r.FormValue("pattern"))) // optional explicit pattern selector

	ext := strings.ToLower(filepath.Ext(fh.Filename))
	if ext == "" {
		// try derive from content-type
		ct := fh.Header.Get("Content-Type")
		switch ct {
		case "video/mp4":
			ext = ".mp4"
		case "video/MP2T", "video/mp2t", "application/octet-stream":
			ext = ".ts"
		case "image/jpeg":
			ext = ".jpg"
		}
	}
	if ext == "" {
		ext = ".dat"
	}

	// Decide which model: event vs snapshot
	if pattern == "event" || (imei != "" && typ != "" && channel != "" && (dtRaw != "")) {
		// Validate IMEI numeric
		if !regexp.MustCompile(`^[0-9]{8,20}$`).MatchString(imei) {
			return "", errors.New("invalid imei")
		}
		if typ != "I" && typ != "F" {
			return "", errors.New("invalid type (expect I or F)")
		}
		if !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(channel) {
			return "", errors.New("invalid channel")
		}

		t, err := parseDateTimeFlexible(dtRaw)
		if err != nil {
			return "", fmt.Errorf("datetime parse: %w", err)
		}
		tsPart := t.Format("2006_01_02_15_04_05")
		reserved := "00000000" // currently constant; can be env in future
		return fmt.Sprintf("EVENT_%s_%s_%s_%s_%s%s", imei, reserved, tsPart, typ, channel, ext), nil
	}

	// Snapshot pattern (heuristic)
	rawHex := strings.TrimSpace(r.FormValue("raw"))
	index := strings.TrimSpace(r.FormValue("index"))
	if pattern == "snapshot" || (imei != "" && rawHex != "" && channel != "" && index != "") {
		if !regexp.MustCompile(`^[0-9]{8,20}$`).MatchString(imei) {
			return "", errors.New("invalid imei")
		}
		if !regexp.MustCompile(`^[0-9A-Fa-f]+$`).MatchString(rawHex) {
			return "", errors.New("invalid raw hex block")
		}
		if !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(channel) {
			return "", errors.New("invalid channel")
		}
		if !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(index) {
			return "", errors.New("invalid index")
		}
		return fmt.Sprintf("%s_%s_%s_%s%s", imei, rawHex, channel, leftPad(index, 2), ext), nil
	}

	return "", errors.New("insufficient data for standardized filename")
}

// parseDateTimeFlexible tries multiple input styles.
func parseDateTimeFlexible(v string) (time.Time, error) {
	if v == "" {
		return time.Now().UTC(), nil
	}
	// Try unix seconds
	if regexp.MustCompile(`^[0-9]{10}$`).MatchString(v) {
		sec, _ := strconv.ParseInt(v, 10, 64)
		return time.Unix(sec, 0).UTC(), nil
	}
	// Compact yyyymmddHHMMSS
	if regexp.MustCompile(`^[0-9]{14}$`).MatchString(v) {
		t, err := time.ParseInLocation("20060102150405", v, time.UTC)
		if err == nil {
			return t, nil
		}
	}
	// RFC3339
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, errors.New("unsupported datetime format")
}

func leftPad(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}
