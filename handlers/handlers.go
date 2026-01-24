package handlers

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"dvr-upload/config"
	"dvr-upload/processor"
	"dvr-upload/queue"
	"dvr-upload/storage"
	"dvr-upload/utils"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type Handler struct {
	cfg               *config.Config
	storage           *storage.StorageService
	rabbitMQ          *queue.RabbitMQClient
	log               *logrus.Logger
	mediaCount        int64
	successfulUploads int64
	failedUploads     int64
	lastUploadTime    int64 // Unix timestamp
	startTime         time.Time
	workerSemaphore   chan struct{}
}

func NewHandler(cfg *config.Config, storage *storage.StorageService, rabbitMQ *queue.RabbitMQClient, log *logrus.Logger) *Handler {
	maxWorkers := cfg.MaxConcurrentWorkers
	if maxWorkers <= 0 {
		maxWorkers = 2 // Default seguro
	}

	return &Handler{
		cfg:             cfg,
		storage:         storage,
		rabbitMQ:        rabbitMQ,
		log:             log,
		startTime:       time.Now(),
		workerSemaphore: make(chan struct{}, maxWorkers),
	}
}

func (h *Handler) HealthHandler(w http.ResponseWriter, r *http.Request) {
	mediaCount := atomic.LoadInt64(&h.mediaCount)
	successCount := atomic.LoadInt64(&h.successfulUploads)
	failCount := atomic.LoadInt64(&h.failedUploads)
	lastTime := atomic.LoadInt64(&h.lastUploadTime)

	s3Status := "ok"
	if err := h.storage.Ping(); err != nil {
		s3Status = fmt.Sprintf("error: %v", err)
	}

	rmqStatus := "ok"
	if h.rabbitMQ != nil {
		if err := h.rabbitMQ.Ping(); err != nil {
			rmqStatus = fmt.Sprintf("error: %v", err)
		}
	} else {
		rmqStatus = "not_configured"
	}

	lastProcessed := "never"
	if lastTime > 0 {
		lastProcessed = time.Unix(lastTime, 0).Format(time.RFC3339)
	}

	status := http.StatusOK
	if s3Status != "ok" || (h.rabbitMQ != nil && rmqStatus != "ok") {
		status = http.StatusServiceUnavailable
	}

	utils.WriteJSON(w, status, utils.JSONResponse{
		Code:    status,
		Message: "health check report",
		Data: map[string]interface{}{
			"status":             "running",
			"uptime":             time.Since(h.startTime).String(),
			"media_total_count":  mediaCount,
			"successful_uploads": successCount,
			"failed_uploads":     failCount,
			"last_processed_at":  lastProcessed,
			"dependencies": map[string]string{
				"s3_storage": s3Status,
				"rabbitmq":   rmqStatus,
			},
		},
	})
}

func (h *Handler) TestPageHandler(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DVR Upload Dashboard</title>
    <style>
        :root {
            --primary: #4f46e5;
            --primary-hover: #4338ca;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text-main: #1e293b;
            --text-muted: #64748b;
            --border: #e2e8f0;
            --success: #22c55e;
        }
        body { 
            font-family: 'Inter', -apple-system, sans-serif; 
            background-color: var(--bg);
            color: var(--text-main);
            max-width: 900px; 
            margin: 0 auto; 
            padding: 40px 20px; 
            line-height: 1.5; 
        }
        .header { text-align: center; margin-bottom: 40px; }
        .header h1 { margin: 0; font-size: 2.25rem; color: var(--text-main); font-weight: 800; letter-spacing: -0.025em; }
        .header p { color: var(--text-muted); margin-top: 8px; }

        .grid { display: grid; grid-template-columns: 1fr; gap: 24px; }
        @media (min-width: 768px) { .grid { grid-template-columns: 3fr 2fr; } }

        .card { 
            background: var(--card-bg); 
            border: 1px solid var(--border);
            padding: 24px; 
            border-radius: 12px; 
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); 
        }
        h2 { margin-top: 0; font-size: 1.25rem; font-weight: 600; display: flex; align-items: center; gap: 8px; }
        
        .form-group { margin-bottom: 20px; }
        label { display: block; font-size: 0.875rem; font-weight: 500; margin-bottom: 6px; color: var(--text-main); }
        
        input[type="text"], input[type="file"] { 
            width: 100%; 
            padding: 10px 12px; 
            border: 1px solid var(--border); 
            border-radius: 6px;
            font-size: 0.95rem;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        input:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.1);
        }
        
        button { 
            background: var(--primary); 
            color: white; 
            border: none; 
            padding: 12px 20px; 
            border-radius: 6px; 
            font-weight: 600;
            cursor: pointer; 
            width: 100%;
            transition: background 0.2s;
        }
        button:hover { background: var(--primary-hover); }
        button.secondary { background: white; color: var(--text-main); border: 1px solid var(--border); margin-top: 12px; }
        button.secondary:hover { background: #f1f5f9; }

        pre { 
            background: #1e293b; 
            color: #e2e8f0;
            padding: 16px; 
            border-radius: 8px; 
            font-size: 0.85rem;
            overflow-x: auto;
            margin-top: 16px;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            padding: 2px 8px;
            border-radius: 99px;
            font-size: 0.75rem;
            font-weight: 600;
            background: #f1f5f9;
        }
        .status-ok { background: #dcfce7; color: #166534; }
    </style>
</head>
<body>
    <div class="header">
        <h1>DVR Upload System</h1>
        <p>Developer Testing Dashboard - Version 2.0.22</p>
    </div>
    
    <div class="grid">
        <div class="card">
            <h2>📤 Simulate Upload</h2>
            <form action="/upload" method="post" enctype="multipart/form-data">
                <div class="form-group">
                    <label>File (TS, MP4 or Images)</label>
                    <input type="file" name="file" required>
                </div>
                <div class="form-group">
                    <label>Filename (Override)</label>
                    <input type="text" name="filename" placeholder="e.g. test_video.mp4">
                    <small style="color:var(--text-muted); font-size:0.75rem">Leave empty to use original filename</small>
                </div>
                <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                    <div class="form-group">
                        <label>Timestamp</label>
                        <input type="text" name="timestamp" id="ts_field">
                    </div>
                    <div class="form-group">
                        <label>IMEI</label>
                        <input type="text" name="imei" id="imei_field" value="864993060014264">
                    </div>
                </div>
                <div class="form-group" id="sign_group" style="display:none;">
                    <label>Signature Token (Auto-generated)</label>
                    <input type="text" name="sign" id="sign_field" readonly style="background:#f1f5f9; cursor:not-allowed;">
                </div>
                <button type="submit">Upload Now</button>
            </form>
        </div>

        <div class="card">
            <h2>🔍 System Health</h2>
            <div id="health_summary" style="margin-bottom: 20px;">
                <p style="font-size:0.9rem; margin: 5px 0;">S3 Storage: <span class="status-badge" id="s3_badge">pending...</span></p>
                <p style="font-size:0.9rem; margin: 5px 0;">RabbitMQ: <span class="status-badge" id="rmq_badge">pending...</span></p>
            </div>
            <button class="secondary" onclick="checkHealth()">Refresh Health Status</button>
            <pre id="health_result">Click refresh to load stats...</pre>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const secretKey = "{{.SecretKey}}";
        const enableSecret = {{.EnableSecret}};

        if (enableSecret) {
            document.getElementById('sign_group').style.display = 'block';
        }

        function generateSignature(updateTimestamp = true) {
            if (!enableSecret) return;
            
            if (updateTimestamp) {
                document.getElementById('ts_field').value = Date.now();
            }
            
            const filenameInput = document.getElementsByName('filename')[0].value;
            const fileInput = document.getElementsByName('file')[0];
            const timestamp = document.getElementById('ts_field').value;
            
            let nameToSign = filenameInput.trim();
            if (!nameToSign && fileInput.files.length > 0) {
                nameToSign = fileInput.files[0].name;
            }

            if (nameToSign) {
                const hash = CryptoJS.MD5(nameToSign + timestamp + secretKey).toString();
                const base64Sign = btoa(hash);
                document.getElementById('sign_field').value = base64Sign;
            }
        }

        document.getElementById('ts_field').value = Date.now();
        
        document.getElementsByName('filename')[0].addEventListener('input', () => generateSignature(true));
        document.getElementsByName('file')[0].addEventListener('change', () => generateSignature(true));
        document.getElementById('ts_field').addEventListener('input', () => generateSignature(false));

        async function checkHealth() {
            try {
                const res = await fetch('/health');
                const data = await res.json();
                document.getElementById('health_result').innerText = JSON.stringify(data, null, 2);
                
                const s3 = data.data.dependencies.s3_storage;
                const rmq = data.data.dependencies.rabbitmq;
                
                const s3Badge = document.getElementById('s3_badge');
                s3Badge.innerText = s3 === 'ok' ? 'Online' : 'Error';
                s3Badge.className = 'status-badge ' + (s3 === 'ok' ? 'status-ok' : '');
                
                const rmqBadge = document.getElementById('rmq_badge');
                rmqBadge.innerText = rmq === 'ok' ? 'Online' : (rmq === 'not_configured' ? 'N/A' : 'Error');
                rmqBadge.className = 'status-badge ' + (rmq === 'ok' ? 'status-ok' : '');
                
            } catch (e) {
                document.getElementById('health_result').innerText = "Failed to connect to /health";
            }
        }

        // Initial check
        checkHealth();
    </script>
</body>
</html>
`
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	// Injetar variáveis do servidor no template HTML
	tmpl := strings.ReplaceAll(html, "{{.SecretKey}}", h.cfg.SecretKey)
	if h.cfg.EnableSecret {
		tmpl = strings.ReplaceAll(tmpl, "{{.EnableSecret}}", "true")
	} else {
		tmpl = strings.ReplaceAll(tmpl, "{{.EnableSecret}}", "false")
	}

	w.Write([]byte(tmpl))
}

func (h *Handler) UploadHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	requestID := uuid.New().String()
	logger := h.log.WithFields(logrus.Fields{
		"request_id":  requestID,
		"remote_addr": r.RemoteAddr,
		"method":      r.Method,
		"uri":         r.RequestURI,
	})

	logger.Info("Upload request received")

	// Usar MultipartReader para processamento mais eficiente de grandes volumes de dados (streaming)
	reader, err := r.MultipartReader()
	if err != nil {
		logger.WithError(err).Error("Failed to create multipart reader")
		utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "Expected multipart/form-data"})
		return
	}

	var streamedTempPath string // Novo: guarda o path do arquivo já em disco
	var handlerFilename string
	var handlerSize int64
	var providedFilename string
	var timestamp string
	var sign string
	var imei string
	var typ string
	var channel string
	var datetime string
	var pattern string
	var raw string
	var index string

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.WithError(err).Error("Failed to read multipart part")
			utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Error reading upload stream"})
			return
		}

		formName := part.FormName()
		if formName == "file" {
			handlerFilename = part.FileName()
			// Determinar diretório base para evitar cross-device links (Rename entre partitions falha)
			tempDir := os.TempDir()
			if h.cfg.EnableLocalStorage {
				if h.cfg.DisasterRecoveryMode && h.cfg.BackupPath != "" {
					tempDir = h.cfg.BackupPath
				} else if h.cfg.VideoPath != "" {
					tempDir = h.cfg.VideoPath
				}
			}
			// Garante que o diretório escolhido existe ou faz fallback para temp do sistema
			if _, err := os.Stat(tempDir); os.IsNotExist(err) && tempDir != os.TempDir() {
				tempDir = os.TempDir()
			}

			tempFile, err := os.CreateTemp(tempDir, "upload-stream-*")
			if err != nil {
				logger.WithError(err).Error("Failed to create temporary file for streaming")
				utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Internal server error"})
				return
			}
			streamedTempPath = tempFile.Name()

			// Usar um buffer maior para io.Copy para melhorar performance de rede/disco
			streamStart := time.Now()
			buffer := make([]byte, 1<<20) // 1MB buffer
			n, err := io.CopyBuffer(tempFile, part, buffer)
			if err != nil {
				tempFile.Close()
				os.Remove(streamedTempPath)
				logger.WithError(err).WithField("bytes_read", n).Error("Error saving file part stream")
				utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Failed to receive file content"})
				return
			}
			handlerSize = n
			tempFile.Close() // Fecha o arquivo pois já terminou a escrita do stream

			duration := time.Since(streamStart)
			speed := float64(n) / 1024 / 1024 / duration.Seconds()
			logger.WithFields(logrus.Fields{
				"size":      n,
				"duration":  duration.String(),
				"speed_mbs": fmt.Sprintf("%.2f MB/s", speed),
			}).Info("File stream received and saved")
			continue // Já processamos o arquivo
		}

		// Processar outros campos do formulário
		value, _ := io.ReadAll(part)
		valStr := string(value)
		switch formName {
		case "filename":
			providedFilename = valStr
		case "timestamp":
			timestamp = valStr
		case "sign":
			sign = valStr
		case "imei":
			imei = valStr
		case "type":
			typ = valStr
		case "channel":
			channel = valStr
		case "datetime":
			datetime = valStr
		case "pattern":
			pattern = valStr
		case "raw":
			raw = valStr
		case "index":
			index = valStr
		}
	}

	if streamedTempPath == "" {
		logger.Error("File is required in the form")
		utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "File is required"})
		return
	}

	fileSize := handlerSize
	logger = logger.WithField("original_filesize", fileSize)

	// Injetar valores lidos para o util de build de filename (BuildStandardFilename espera http.Request populado)
	// Como usamos MultipartReader, r.FormValue não funciona. Precisamos de um r falso ou ajustar o util.
	// Vamos ajustar o mock do r para que o BuildStandardFilename funcione ou extrair a lógica.

	// Para não quebrar o utils.BuildStandardFilename que usa r.FormValue, vamos popular r.Form temporariamente
	// se possível, mas r.Form é privado. Vamos usar um workaround ou duplicar lógica simples.

	// Alternativa: o BuildStandardFilename usa r.FormValue. Podemos injetar os campos.
	r.Form = make(url.Values)
	r.Form.Set("imei", imei)
	r.Form.Set("type", typ)
	r.Form.Set("channel", channel)
	r.Form.Set("datetime", datetime)
	r.Form.Set("pattern", pattern)
	r.Form.Set("raw", raw)
	r.Form.Set("index", index)

	builtName, buildErr := utils.BuildStandardFilename(r, &multipart.FileHeader{Filename: handlerFilename, Size: handlerSize})
	finalFilename := strings.TrimSpace(providedFilename)
	if finalFilename == "" {
		if buildErr == nil && builtName != "" {
			finalFilename = builtName
		} else {
			finalFilename = handlerFilename
		}
	}
	finalFilename = filepath.Base(finalFilename)

	fields := logrus.Fields{
		"original_filename": handlerFilename,
		"final_filename":    finalFilename,
		"provided_filename": providedFilename,
		"timestamp":         timestamp,
	}
	if buildErr != nil && strings.TrimSpace(providedFilename) == "" {
		fields["build_error"] = buildErr.Error()
	}

	reqLogger := logger.WithFields(fields)
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

	// Usa um caminho temporário único para evitar colisões
	tempPath := savedPath + "." + requestID + ".tmp"

	// Garantir que o diretório de destino existe
	if err := os.MkdirAll(filepath.Dir(tempPath), 0755); err != nil {
		reqLogger.WithError(err).Error("Failed to create destination directory")
		utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Internal server error"})
		return
	}

	// Move o arquivo já streamado para o caminho de processamento (sem double IO)
	if err := os.Rename(streamedTempPath, tempPath); err != nil {
		reqLogger.WithError(err).Warn("Failed to rename streamed file, attempting copy fallback")
		// Fallback para cópia se estiverem em dispositivos diferentes
		if err := utils.CopyFile(streamedTempPath, tempPath); err != nil {
			reqLogger.WithError(err).Error("Fallback copy failed")
			utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Failed to save file"})
			return
		}
		os.Remove(streamedTempPath)
	}
	bytesWritten = fileSize

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

	go func(path string, filename string, originalSavedPath string, logger *logrus.Entry, isLocal bool, startTime time.Time, initialSize int64) {
		// Adquire semáforo para limitar processamento paralelo (FFmpeg consome muita RAM)
		h.workerSemaphore <- struct{}{}
		defer func() { <-h.workerSemaphore }()

		bgStart := time.Now()
		uploadPath := path
		uploadFilename := filename
		currentSize := initialSize
		ext := strings.ToLower(filepath.Ext(filename))

		logger.WithFields(logrus.Fields{
			"bg_start_delay": bgStart.Sub(startTime).String(),
			"initial_size":   initialSize,
		}).Info("Background processing started")

		// Conversão opcional TS -> MP4 antes do upload.
		if ext == ".ts" && h.cfg.EnableTsToMp4 {
			convStart := time.Now()
			convertedPath, err := processor.ConvertTSToMP4(path, logger)
			if err == nil {
				// Captura o novo tamanho após conversão
				if stat, statErr := os.Stat(convertedPath); statErr == nil {
					currentSize = stat.Size()
					// Remove o arquivo temporário original se a conversão funcionou e temos o novo arquivo
					os.Remove(path)
					uploadPath = convertedPath
					uploadFilename = strings.TrimSuffix(filename, ext) + ".mp4"
					ext = ".mp4" // Atualiza extensão para o próximo passo (compressão)
					logger.WithFields(logrus.Fields{
						"duration": time.Since(convStart).String(),
						"new_size": currentSize,
					}).Info("TS->MP4 conversion background step done")
				} else {
					logger.WithError(statErr).Warn("TS->MP4 conversion succeeded but could not stat result, keeping original")
					os.Remove(convertedPath)
				}
			} else {
				logger.WithError(err).Warn("TS->MP4 conversion failed, will attempt to upload original as TS")
				// Se falhar a conversão, mantemos o arquivo original .ts para upload
			}
		}

		// Mantém a compressão atual apenas para MP4 (se aplicável).
		// Se o arquivo original era JPEG ou outro formato, ele pulará este bloco e irá direto para S3
		if ext == ".mp4" {
			compStart := time.Now()
			compressedPath, err := processor.CompressWithFFmpeg(uploadPath, logger)
			if err == nil {
				// Comparar tamanhos: se o comprimido for maior que o original (comum com CRF 0 ou arquivos pequenos),
				// descartamos o comprimido e usamos o original.
				origStat, errOrig := os.Stat(uploadPath)
				compStat, errComp := os.Stat(compressedPath)

				if errOrig == nil && errComp == nil {
					if compStat.Size() < origStat.Size() && compStat.Size() > 0 {
						preCompSize := origStat.Size()
						compSize := compStat.Size()
						os.Remove(uploadPath)
						uploadPath = compressedPath
						currentSize = compSize

						reduction := float64(preCompSize-compSize) / float64(preCompSize) * 100
						globalReduction := float64(initialSize-compSize) / float64(initialSize) * 100

						logger.WithFields(logrus.Fields{
							"duration":             time.Since(compStart).String(),
							"pre_compression_size": preCompSize,
							"compressed_size":      compSize,
							"step_reduction":       fmt.Sprintf("%.2f%%", reduction),
							"global_reduction":     fmt.Sprintf("%.2f%%", globalReduction),
						}).Info("Compression background step done (using compressed)")
					} else {
						logger.WithFields(logrus.Fields{
							"duration":        time.Since(compStart).String(),
							"original_size":   origStat.Size(),
							"compressed_size": compStat.Size(),
						}).Info("Compressed file is larger than original or empty, keeping original")
						os.Remove(compressedPath)
					}
				} else {
					logger.Warn("Failed to stat files for compression comparison, keeping current")
					os.Remove(compressedPath)
				}
			} else {
				logger.WithError(err).Warn("Compression failed, will attempt to upload original")
			}
		}

		s3Start := time.Now()
		if err := h.storage.UploadFileToS3(uploadPath, uploadFilename, logger); err != nil {
			logger.WithError(err).Error("Failed to upload to S3")
			atomic.AddInt64(&h.failedUploads, 1)
			// Remove o arquivo temporário mesmo em caso de falha no S3
			os.Remove(uploadPath)
			return
		}
		logger.WithField("duration", time.Since(s3Start).String()).Info("S3 upload background step done")

		atomic.AddInt64(&h.successfulUploads, 1)
		atomic.StoreInt64(&h.lastUploadTime, time.Now().Unix())

		finalDestPath := uploadPath
		renameStart := time.Now()
		if isLocal {
			// Define o caminho final baseado no nome final do arquivo (pode ter mudado de .ts para .mp4)
			finalDestPath = filepath.Join(filepath.Dir(originalSavedPath), uploadFilename)
			if err := os.Rename(uploadPath, finalDestPath); err != nil {
				logger.WithError(err).Warn("Failed to move temporary file to final destination, keeping temporary path")
				finalDestPath = uploadPath
			} else {
				logger.WithField("duration", time.Since(renameStart).String()).Info("File moved to final local destination")
			}
		} else {
			if _, err := os.Stat(uploadPath); err == nil {
				os.Remove(uploadPath)
			}
			finalDestPath = ""
		}

		// Incrementa contador e dispara evento RabbitMQ apenas após sucesso no upload
		atomic.AddInt64(&h.mediaCount, 1)

		if h.rabbitMQ != nil {
			logger.WithFields(logrus.Fields{
				"final_filename": uploadFilename,
				"final_size":     currentSize,
				"exchange":       h.cfg.RabbitMQExchange,
				"queue":          h.cfg.RabbitMQQueue,
			}).Info("Publishing event to RabbitMQ")
			err := h.rabbitMQ.PublishEvent(queue.UploadEvent{
				Filename: uploadFilename,
				Size:     currentSize,
				Path:     finalDestPath,
			})
			if err != nil {
				logger.WithError(err).Error("Failed to publish event to RabbitMQ after processing")
			}
		}
		logger.WithFields(logrus.Fields{
			"total_duration": time.Since(startTime).String(),
			"bg_duration":    time.Since(bgStart).String(),
		}).Info("Background processing finished successfully")
	}(tempPath, finalFilename, savedPath, reqLogger, h.cfg.EnableLocalStorage, startTime, fileSize)

	utils.WriteJSON(w, http.StatusOK, utils.JSONResponse{Code: 200, Message: "File upload success", Data: finalFilename})
}
