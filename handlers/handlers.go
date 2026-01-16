package handlers

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"dvr-upload/config"
	"dvr-upload/processor"
	"dvr-upload/queue"
	"dvr-upload/storage"
	"dvr-upload/utils"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type Handler struct {
	cfg        *config.Config
	storage    *storage.StorageService
	rabbitMQ   *queue.RabbitMQClient
	log        *logrus.Logger
	mediaCount int64
}

func NewHandler(cfg *config.Config, storage *storage.StorageService, rabbitMQ *queue.RabbitMQClient, log *logrus.Logger) *Handler {
	return &Handler{
		cfg:      cfg,
		storage:  storage,
		rabbitMQ: rabbitMQ,
		log:      log,
	}
}

func (h *Handler) HealthHandler(w http.ResponseWriter, r *http.Request) {
	count := atomic.LoadInt64(&h.mediaCount)
	utils.WriteJSON(w, http.StatusOK, utils.JSONResponse{
		Code:    200,
		Message: "ok",
		Data: map[string]interface{}{
			"media_processed_count": count,
		},
	})
}

func (h *Handler) UploadHandler(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()
	logger := h.log.WithFields(logrus.Fields{
		"request_id":  requestID,
		"remote_addr": r.RemoteAddr,
		"method":      r.Method,
		"uri":         r.RequestURI,
	})

	logger.Info("Upload request received")

	if err := r.ParseMultipartForm(64 << 20); err != nil {
		logger.WithError(err).Error("Failed to parse multipart form")
		utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "Invalid form data"})
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		logger.WithError(err).Error("File is required in the form")
		utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "File is required"})
		return
	}
	defer file.Close()

	fileSize := handler.Size
	logger = logger.WithField("original_filesize", fileSize)

	providedFilename := r.FormValue("filename")
	timestamp := r.FormValue("timestamp")
	sign := r.FormValue("sign")

	builtName, buildErr := utils.BuildStandardFilename(r, handler)
	finalFilename := strings.TrimSpace(providedFilename)
	if finalFilename == "" {
		if buildErr == nil && builtName != "" {
			finalFilename = builtName
		} else {
			finalFilename = handler.Filename
		}
	}
	finalFilename = filepath.Base(finalFilename)

	reqLogger := logger.WithFields(logrus.Fields{
		"original_filename": handler.Filename,
		"final_filename":    finalFilename,
		"provided_filename": providedFilename,
		"timestamp":         timestamp,
		"build_error":       buildErr,
	})
	reqLogger.Info("Processing file upload")

	if len(finalFilename) > utils.MaxFilenameLength {
		reqLogger.Error("Final filename exceeds max length")
		utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "File name too long"})
		return
	}

	if h.cfg.EnableSecret {
		baseForSign := providedFilename
		if strings.TrimSpace(baseForSign) == "" {
			baseForSign = finalFilename
		}
		expected := utils.GenerateSign(baseForSign, timestamp, h.cfg.SecretKey)
		if sign != expected {
			reqLogger.WithFields(logrus.Fields{
				"received_sign": sign,
				"expected_sign": expected,
				"base_for_sign": baseForSign,
			}).Warn("Invalid signature")
			utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "Signature error"})
			return
		}
		reqLogger.Info("Signature validated successfully")
	}

	var bytesWritten int64
	var savedPath string
	if !h.cfg.EnableLocalStorage {
		savedPath = filepath.Join(os.TempDir(), finalFilename)
	} else if h.cfg.DisasterRecoveryMode {
		if h.cfg.BackupPath == "" {
			reqLogger.Error("Disaster Recovery mode is ON but BACKUP_VIDEO_PATH is not set.")
			utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Disaster recovery misconfigured"})
			return
		}
		savedPath = filepath.Join(h.cfg.BackupPath, finalFilename)
	} else {
		savedPath = filepath.Join(h.cfg.VideoPath, finalFilename)
	}

	// Usa um caminho temporário único para evitar colisões durante o processamento concorrente
	tempPath := savedPath + "." + requestID + ".tmp"
	bytesWritten, err = h.storage.SaveUploadedFile(file, tempPath, reqLogger)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: err.Error()})
		return
	}

	if fileSize != bytesWritten {
		reqLogger.Warnf("File size mismatch. Original: %d, Written: %d. Possible incomplete upload.", fileSize, bytesWritten)
	}

	if h.cfg.EnableLocalStorage {
		reqLogger.WithFields(logrus.Fields{
			"temp_path":     tempPath,
			"bytes_written": bytesWritten,
		}).Info("Upload success (local storage ON - temporary file created)")
	} else {
		reqLogger.WithFields(logrus.Fields{
			"temp_path":     tempPath,
			"bytes_written": bytesWritten,
		}).Info("Upload success (RAM/Temp storage)")
	}

	go func(path string, filename string, originalSavedPath string, logger *logrus.Entry, isLocal bool) {
		uploadPath := path
		uploadFilename := filename
		ext := strings.ToLower(filepath.Ext(filename))

		// Conversão opcional TS -> MP4 antes do upload.
		if ext == ".ts" && h.cfg.EnableTsToMp4 {
			convertedPath, err := processor.ConvertTSToMP4(path, logger)
			if err == nil {
				// Remove o arquivo temporário original se a conversão funcionou
				os.Remove(path)
				uploadPath = convertedPath
				uploadFilename = strings.TrimSuffix(filename, ext) + ".mp4"
				ext = ".mp4" // Atualiza extensão para o próximo passo (compressão)
			} else {
				logger.WithError(err).Warn("TS->MP4 conversion failed, will attempt to upload original")
			}
		}

		// Mantém a compressão atual apenas para MP4 (se aplicável).
		if ext == ".mp4" {
			compressedPath, err := processor.CompressWithFFmpeg(uploadPath, logger)
			if err == nil {
				// Substitui o arquivo temporário pelo comprimido
				os.Remove(uploadPath)
				uploadPath = compressedPath
			} else {
				logger.WithError(err).Warn("Compression failed, will attempt to upload original")
			}
		}

		if err := h.storage.UploadFileToS3(uploadPath, uploadFilename, logger); err != nil {
			logger.WithError(err).Error("Failed to upload to S3")
			// Remove o arquivo temporário mesmo em caso de falha no S3
			os.Remove(uploadPath)
			return
		}

		finalDestPath := uploadPath
		if isLocal {
			// Define o caminho final baseado no nome final do arquivo (pode ter mudado de .ts para .mp4)
			finalDestPath = filepath.Join(filepath.Dir(originalSavedPath), uploadFilename)
			if err := os.Rename(uploadPath, finalDestPath); err != nil {
				logger.WithError(err).Warn("Failed to move temporary file to final destination, keeping temporary path")
				finalDestPath = uploadPath
			}
		} else {
			if _, err := os.Stat(uploadPath); err == nil {
				os.Remove(uploadPath)
			}
		}

		// Incrementa contador e dispara evento RabbitMQ apenas após sucesso no upload
		atomic.AddInt64(&h.mediaCount, 1)

		if h.rabbitMQ != nil {
			finalSize := int64(0)
			if stat, err := os.Stat(finalDestPath); err == nil {
				finalSize = stat.Size()
			}
			err := h.rabbitMQ.PublishEvent(queue.UploadEvent{
				Filename: uploadFilename,
				Size:     finalSize,
				Path:     finalDestPath,
			})
			if err != nil {
				logger.WithError(err).Error("Failed to publish event to RabbitMQ after processing")
			}
		}
	}(tempPath, finalFilename, savedPath, reqLogger, h.cfg.EnableLocalStorage)

	utils.WriteJSON(w, http.StatusOK, utils.JSONResponse{Code: 200, Message: "File upload success", Data: finalFilename})
}
