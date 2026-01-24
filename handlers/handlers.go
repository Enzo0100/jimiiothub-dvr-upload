package handlers

import (
	"fmt"
	"io"
	"log/slog"
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
)

type Handler struct {
	cfg               *config.Config
	storage           *storage.StorageService
	rabbitMQ          *queue.RabbitMQClient
	log               *slog.Logger
	mediaCount        int64
	successfulUploads int64
	failedUploads     int64
	lastUploadTime    int64 // Unix timestamp
	startTime         time.Time
	workerSemaphore   chan struct{}
}

func NewHandler(cfg *config.Config, storage *storage.StorageService, rabbitMQ *queue.RabbitMQClient, log *slog.Logger) *Handler {
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
	resultStatus := "nak" // Default is NAK (Negative Acknowledgement)

	defer func() {
		endTime := time.Now()
		h.log.Info("POST request summary",
			"request_id", requestID,
			"arrival_time", startTime.Format(time.RFC3339),
			"completion_time", endTime.Format(time.RFC3339),
			"duration", endTime.Sub(startTime).String(),
			"result", resultStatus,
		)
	}()

	logger := h.log.With(
		"request_id", requestID,
		"remote_addr", r.RemoteAddr,
		"method", r.Method,
		"uri", r.RequestURI,
	)

	// Usar MultipartReader para processamento mais eficiente de grandes volumes de dados (streaming)
	reader, err := r.MultipartReader()
	if err != nil {
		logger.Error("Failed to create multipart reader", "error", err)
		utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "Expected multipart/form-data"})
		return
	}

	var streamedTempPath string // Novo: guarda o path do arquivo já em disco
	defer func() {
		if streamedTempPath != "" {
			if _, err := os.Stat(streamedTempPath); err == nil {
				os.Remove(streamedTempPath)
			}
		}
	}()

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
			// Se já recebemos o arquivo com sucesso, ignoramos erros em partes subsequentes (podem ser campos opcionais que falharam por timeout)
			if streamedTempPath != "" {
				logger.Warn("Connection interrupted or error reading next part, but file was already received. Proceeding.", "error", err)
				break
			}
			logger.Error("Failed to read multipart part", "error", err)
			utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Error reading upload stream"})
			return
		}

		formName := part.FormName()
		if formName == "file" {
			handlerFilename = part.FileName()
			// Determinar diretório base e usar subpasta oculta para isolar do coletor externo
			baseDir := os.TempDir()
			if h.cfg.EnableLocalStorage {
				if h.cfg.DisasterRecoveryMode && h.cfg.BackupPath != "" {
					baseDir = h.cfg.BackupPath
				} else if h.cfg.VideoPath != "" {
					baseDir = h.cfg.VideoPath
				}
			}

			// Pasta de processamento oculta para evitar que coletores externos peguem arquivos incompletos
			tempDir := filepath.Join(baseDir, ".processing")
			if err := os.MkdirAll(tempDir, 0755); err != nil {
				// Se falhar ao criar a oculta, usa o baseDir mas loga o aviso
				logger.Warn("Failed to create hidden processing dir, using base dir", "path", tempDir, "error", err)
				tempDir = baseDir
			}

			tempFile, err := os.CreateTemp(tempDir, "upload-stream-*")
			if err != nil {
				logger.Error("Failed to create temporary file for streaming", "error", err)
				utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Internal server error"})
				return
			}
			streamedTempPath = tempFile.Name()

			// Usar um buffer maior para io.Copy para melhorar performance de rede/disco
			buffer := make([]byte, 1<<20) // 1MB buffer
			n, err := io.CopyBuffer(tempFile, part, buffer)
			if err != nil {
				tempFile.Close()
				os.Remove(streamedTempPath)
				streamedTempPath = "" // Reseta para o defer não tentar remover de novo
				logger.Error("Error saving file part stream", "bytes_read", n, "error", err)
				utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Failed to receive file content"})
				return
			}
			handlerSize = n
			tempFile.Close() // Fecha o arquivo pois já terminou a escrita do stream
			continue         // Já processamos o arquivo
		}

		// Processar outros campos do formulário
		value, errReadAll := io.ReadAll(part)
		if errReadAll != nil {
			logger.Warn("Error reading form field", "field", formName, "error", errReadAll)
			continue
		}
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
	logger = logger.With("original_filesize", fileSize)

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

	reqLogger := logger.With(
		"original_filename", handlerFilename,
		"final_filename", finalFilename,
		"provided_filename", providedFilename,
		"timestamp", timestamp,
	)
	if buildErr != nil && strings.TrimSpace(providedFilename) == "" {
		reqLogger = reqLogger.With("build_error", buildErr.Error())
	}

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
			reqLogger.Warn("Invalid signature",
				"received_sign", sign,
				"expected_sign", expected,
				"base_for_sign", baseForSign)
			utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "Signature error"})
			return
		}
	}

	var savedPath string
	var processingPath string

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

	// Define path de processamento isolado (na mesma partição para rename rápido)
	processingBase := filepath.Join(filepath.Dir(savedPath), ".processing")
	os.MkdirAll(processingBase, 0755)
	processingPath = filepath.Join(processingBase, finalFilename+"."+requestID+".tmp")

	// Move o arquivo já streamado para o caminho de processamento isolado
	if err := os.Rename(streamedTempPath, processingPath); err == nil {
		streamedTempPath = "" // Limpa para o defer não remover o arquivo movido
	} else {
		reqLogger.Warn("Failed to rename streamed file to processing dir, attempting copy", "error", err)
		if err := utils.CopyFile(streamedTempPath, processingPath); err != nil {
			reqLogger.Error("Processing copy failed", "error", err)
			utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Failed to save file"})
			return
		}
		os.Remove(streamedTempPath)
		streamedTempPath = ""
	}

	go func(path string, filename string, targetFinalPath string, logger *slog.Logger, isLocal bool, startTime time.Time, initialSize int64) {
		// Adquire semáforo para limitar processamento paralelo
		h.workerSemaphore <- struct{}{}

		uploadPath := path
		uploadFilename := filename
		currentSize := initialSize
		ext := strings.ToLower(filepath.Ext(filename))

		defer func() {
			if r := recover(); r != nil {
				logger.Error("Panic in processing goroutine", "panic", r)
			}
			// Cleanup: remove arquivo de processamento se ainda existir e não for local
			if uploadPath != "" && strings.Contains(uploadPath, ".processing") {
				if _, err := os.Stat(uploadPath); err == nil {
					os.Remove(uploadPath)
				}
			}
			<-h.workerSemaphore
		}()

		// ...resto do processamento...

		// Conversão opcional TS -> MP4 antes do upload.
		if ext == ".ts" && h.cfg.EnableTsToMp4 {
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
				} else {
					logger.Warn("TS->MP4 conversion succeeded but could not stat result", "error", statErr)
					os.Remove(convertedPath)
				}
			} else {
				logger.Warn("TS->MP4 conversion failed, will attempt to upload original as TS", "error", err)
				// Se falhar a conversão, mantemos o arquivo original .ts para upload
			}
		}

		// Mantém a compressão atual apenas para MP4 (se aplicável).
		// Se o arquivo original era JPEG ou outro formato, ele pulará este bloco e irá direto para S3
		if ext == ".mp4" {
			compressedPath, err := processor.CompressWithFFmpeg(uploadPath, logger)
			if err == nil {
				// Comparar tamanhos: se o comprimido for maior que o original (comum com CRF 0 ou arquivos pequenos),
				// descartamos o comprimido e usamos o original.
				origStat, errOrig := os.Stat(uploadPath)
				compStat, errComp := os.Stat(compressedPath)

				if errOrig == nil && errComp == nil {
					if compStat.Size() < origStat.Size() && compStat.Size() > 0 {
						compSize := compStat.Size()
						os.Remove(uploadPath)
						uploadPath = compressedPath
						currentSize = compSize
					} else {
						os.Remove(compressedPath)
					}
				} else {
					os.Remove(compressedPath)
				}
			}
		}

		if h.cfg.EnableS3Upload {
			if err := h.storage.UploadFileToS3(uploadPath, uploadFilename, logger); err != nil {
				logger.Error("Failed to upload to S3", "error", err)
				atomic.AddInt64(&h.failedUploads, 1)
				return
			}
			atomic.AddInt64(&h.successfulUploads, 1)
		} else {
			logger.Info("S3 upload skipped (disabled by config)")
			// Consideramos sucesso local se o upload está desabilitado
			atomic.AddInt64(&h.successfulUploads, 1)
		}

		atomic.StoreInt64(&h.lastUploadTime, time.Now().Unix())

		finalDestPath := uploadPath
		if isLocal {
			// Define o caminho final baseado no nome final do arquivo (pode ter mudado de .ts para .mp4)
			finalDestPath = filepath.Join(filepath.Dir(targetFinalPath), uploadFilename)

			// Se o diretório destino não existe, cria (caso tenha sido removido por outro serviço)
			os.MkdirAll(filepath.Dir(finalDestPath), 0755)

			if err := os.Rename(uploadPath, finalDestPath); err != nil {
				logger.Warn("Failed to move temporary file to final destination, attempting copy", "error", err)
				if copyErr := utils.CopyFile(uploadPath, finalDestPath); copyErr == nil {
					os.Remove(uploadPath)
				} else {
					logger.Error("Final move/copy failed", "error", copyErr)
					finalDestPath = uploadPath
				}
			}
		} else {
			finalDestPath = ""
		}

		// Incrementa contador e dispara evento RabbitMQ apenas após sucesso no upload
		atomic.AddInt64(&h.mediaCount, 1)

		if h.cfg.EnableRabbitMQ && h.rabbitMQ != nil {
			err := h.rabbitMQ.PublishEvent(queue.UploadEvent{
				Filename: uploadFilename,
				Size:     currentSize,
				Path:     finalDestPath,
			})
			if err != nil {
				logger.Error("Failed to publish event to RabbitMQ after processing", "error", err)
			}
		}
		logger.Info("Upload and processing finished",
			"filename", uploadFilename,
			"size", currentSize,
			"total_duration", time.Since(startTime).String())
	}(processingPath, finalFilename, savedPath, reqLogger, h.cfg.EnableLocalStorage, startTime, fileSize)

	resultStatus = "ack" // Mark as ACK (Acknowledgement) for the summary log
	utils.WriteJSON(w, http.StatusOK, utils.JSONResponse{Code: 200, Message: "File upload success", Data: finalFilename})
}
