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
	cfg                *config.Config
	storage            *storage.StorageService
	rabbitMQ           *queue.RabbitMQClient
	log                *slog.Logger
	mediaCount         int64
	successfulUploads  int64
	failedUploads      int64
	interruptedUploads int64
	totalIncoming      int64
	activeUploads      int64
	waitingProcessors  int64
	activeProcessors   int64
	lastUploadTime     int64 // Unix timestamp
	startTime          time.Time
	workerSemaphore    chan struct{}

	// Métricas de tempo (em nanosegundos para precisão no atomic)
	totalConversionTime int64
	conversionCount     int64
	totalS3UploadTime   int64
	s3UploadCount       int64
	totalCameraSendTime int64
	cameraSendCount     int64
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
	interruptedCount := atomic.LoadInt64(&h.interruptedUploads)
	totalIncoming := atomic.LoadInt64(&h.totalIncoming)
	activeUploads := atomic.LoadInt64(&h.activeUploads)
	activeProcessors := atomic.LoadInt64(&h.activeProcessors)
	waitingProcessors := atomic.LoadInt64(&h.waitingProcessors)

	lastTime := atomic.LoadInt64(&h.lastUploadTime)

	// Cálculos de médias de tempo
	var avgCameraSend, avgConversion, avgS3Upload string

	camCount := atomic.LoadInt64(&h.cameraSendCount)
	if camCount > 0 {
		avgCameraSend = (time.Duration(atomic.LoadInt64(&h.totalCameraSendTime)) / time.Duration(camCount)).String()
	} else {
		avgCameraSend = "0s"
	}

	convCount := atomic.LoadInt64(&h.conversionCount)
	if convCount > 0 {
		avgConversion = (time.Duration(atomic.LoadInt64(&h.totalConversionTime)) / time.Duration(convCount)).String()
	} else {
		avgConversion = "0s"
	}

	s3Count := atomic.LoadInt64(&h.s3UploadCount)
	if s3Count > 0 {
		avgS3Upload = (time.Duration(atomic.LoadInt64(&h.totalS3UploadTime)) / time.Duration(s3Count)).String()
	} else {
		avgS3Upload = "0s"
	}

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
			"status":              "running",
			"uptime":              time.Since(h.startTime).String(),
			"media_total_count":   mediaCount,
			"successful_uploads":  successCount,
			"failed_uploads":      failCount,
			"interrupted_uploads": interruptedCount,
			"total_incoming":      totalIncoming,
			"active_uploads":      activeUploads,
			"active_processors":   activeProcessors,
			"waiting_processors":  waitingProcessors,
			"last_processed_at":   lastProcessed,
			"metrics": map[string]string{
				"avg_camera_send_time": avgCameraSend,
				"avg_conversion_time":  avgConversion,
				"avg_s3_upload_time":   avgS3Upload,
			},
			"dependencies": map[string]string{
				"s3_storage": s3Status,
				"rabbitmq":   rmqStatus,
			},
		},
	})
}

func (h *Handler) TestPageHandler(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DVR Monitor</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; background-color: #0f172a; color: #f8fafc; }
        .stat-card { background: #1e293b; border: 1px solid #334155; transition: all 0.3s ease; }
        .stat-card:hover { border-color: #4f46e5; transform: translateY(-2px); }
    </style>
</head>
<body class="p-4 md:p-8">
    <div class="max-w-7xl mx-auto">
        <div class="flex flex-col md:flex-row justify-between items-center mb-10 gap-6">
            <div>
                <div class="flex items-center gap-3">
                    <div class="p-2 bg-indigo-600 rounded-lg">
                        <svg class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path></svg>
                    </div>
                    <h1 class="text-3xl font-bold text-white tracking-tight">DVR Service Monitor</h1>
                </div>
                <p class="text-slate-400 mt-2 ml-1">Performance em tempo real e sincronização em nuvem</p>
            </div>
            
            <div class="flex items-center bg-slate-800/50 p-2 rounded-2xl border border-slate-700">
                <div class="px-4 border-r border-slate-700">
                    <div class="flex items-center gap-2">
                        <span id="status-dot" class="h-2.5 w-2.5 rounded-full bg-green-500 animate-pulse"></span>
                        <span id="status-text" class="text-xs font-bold uppercase tracking-widest text-slate-300">Operacional</span>
                    </div>
                </div>
                <div class="px-4 text-xs font-mono text-indigo-400">
                    Up: <span id="uptime">0s</span>
                </div>
                <button onclick="refreshData()" class="ml-2 p-2 hover:bg-slate-700 rounded-xl transition-colors text-slate-400 hover:text-white">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
                </button>
            </div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-5 mb-8">
            <div class="bg-indigo-600/10 border border-indigo-500/20 p-6 rounded-3xl flex items-center justify-between">
                <div>
                    <p class="text-[10px] font-bold text-indigo-400 uppercase tracking-widest">Total de Requisições</p>
                    <h2 id="total-incoming" class="text-4xl font-black mt-2 text-white">0</h2>
                    <p class="text-[10px] text-slate-500 mt-1">Total de arquivos recebidos via POST</p>
                </div>
                <div class="bg-indigo-600/20 p-4 rounded-2xl">
                    <svg class="w-8 h-8 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
                </div>
            </div>

            <div class="bg-purple-600/10 border border-purple-500/20 p-6 rounded-3xl flex items-center justify-between">
                <div>
                    <p class="text-[10px] font-bold text-purple-400 uppercase tracking-widest">Ativos Agora</p>
                    <div class="flex items-center gap-3 mt-2">
                        <span class="relative flex h-3 w-3">
                            <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-purple-400 opacity-75"></span>
                            <span class="relative inline-flex rounded-full h-3 w-3 bg-purple-500"></span>
                        </span>
                        <h2 id="active-processors" class="text-4xl font-black text-white">0</h2>
                        <div class="ml-4 border-l border-purple-500/30 pl-4">
                            <p class="text-[10px] font-bold text-slate-500 uppercase">Aguardando CPU</p>
                            <p id="waiting-processors" class="text-xl font-bold text-purple-300">0</p>
                        </div>
                        <div class="ml-4 border-l border-purple-500/30 pl-4">
                            <p class="text-[10px] font-bold text-indigo-500 uppercase">Em Transmissão</p>
                            <p id="active-uploads" class="text-xl font-bold text-indigo-300">0</p>
                        </div>
                    </div>
                </div>
                <div class="bg-purple-600/20 p-4 rounded-2xl">
                    <svg class="w-8 h-8 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                </div>
            </div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-5 mb-8">
            <div class="stat-card p-6 rounded-3xl relative overflow-hidden">
                <p class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Total de Arquivos</p>
                <h2 id="media-total" class="text-4xl font-black mt-2 text-white">0</h2>
                <div class="mt-4 flex items-center gap-2">
                    <span class="text-[10px] text-green-400 font-bold" id="success-count">OK: 0</span>
                    <span class="text-[10px] text-red-400 font-bold" id="fail-count">FAIL: 0</span>
                    <span class="text-[10px] text-orange-400 font-bold" title="Upload da câmera interrompido">INT: <span id="interrupted-count">0</span></span>
                    <span class="text-[10px] text-slate-400 ml-auto font-bold" id="success-rate">0%</span>
                </div>
            </div>

            <div class="stat-card p-6 rounded-3xl">
                <p class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Latência Câmera</p>
                <h2 id="avg-camera" class="text-4xl font-black text-indigo-400 mt-2">0s</h2>
                <p class="text-[10px] text-slate-500 mt-4 italic">Média de envio da câmera</p>
            </div>

            <div class="stat-card p-6 rounded-3xl">
                <p class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Processamento</p>
                <h2 id="avg-conversion" class="text-4xl font-black text-purple-400 mt-2">0s</h2>
                <p class="text-[10px] text-slate-500 mt-4 italic">Média CPU FFmpeg</p>
            </div>

            <div class="stat-card p-6 rounded-3xl">
                <p class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Upload Nuvem</p>
                <h2 id="avg-s3" class="text-4xl font-black text-emerald-400 mt-2">0s</h2>
                <p class="text-[10px] text-slate-500 mt-4 italic">Média Latência S3</p>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div class="stat-card p-8 rounded-3xl lg:col-span-1">
                <h3 class="text-sm font-bold mb-6 text-white uppercase tracking-widest">Integridade</h3>
                <div class="space-y-4">
                    <div class="flex justify-between items-center p-3 bg-slate-800/40 rounded-xl border border-slate-700/50">
                        <span class="text-xs text-slate-400">S3 Storage</span>
                        <span id="badge-s3" class="text-[10px] font-black px-2 py-1 rounded-md uppercase">...</span>
                    </div>
                    <div class="flex justify-between items-center p-3 bg-slate-800/40 rounded-xl border border-slate-700/50">
                        <span class="text-xs text-slate-400">RabbitMQ</span>
                        <span id="badge-rmq" class="text-[10px] font-black px-2 py-1 rounded-md uppercase">...</span>
                    </div>
                    <div class="pt-4 border-t border-slate-700/50 text-right">
                        <span class="text-[10px] text-slate-500 uppercase tracking-widest">Última Atividade:</span>
                        <p id="last-at" class="text-[10px] font-mono text-slate-400">Never</p>
                    </div>
                </div>
            </div>

            <div class="stat-card p-8 rounded-3xl lg:col-span-2 bg-gradient-to-br from-slate-800 to-slate-900 border-indigo-500/20">
                <h3 class="text-sm font-bold mb-6 text-white uppercase tracking-widest">Simular Upload</h3>
                <form id="uploadForm" class="space-y-4">
                    <div class="grid grid-cols-2 gap-4">
                        <input type="file" name="file" required class="col-span-2 text-xs text-slate-400 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-[10px] file:font-bold file:bg-indigo-600 file:text-white bg-slate-800/50 rounded-lg p-1">
                        <input type="text" name="filename" placeholder="Override Filename" class="bg-slate-800/50 border border-slate-700 rounded-lg p-2 text-xs text-white outline-none focus:border-indigo-500">
                        <input type="text" name="imei" value="864993060014264" class="bg-slate-800/50 border border-slate-700 rounded-lg p-2 text-xs text-white outline-none">
                    </div>
                    <button type="submit" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2 rounded-lg text-xs transition-all active:scale-[0.98]">ENVIAR ARQUIVO</button>
                    <div id="uploadStatus" class="mt-4 p-3 bg-black/40 rounded-lg hidden border border-white/5 font-mono text-[10px] text-indigo-400">
                        <pre id="responseOutput" class="whitespace-pre-wrap"></pre>
                    </div>
                </form>
            </div>
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js"></script>
    <script>
        const secretKey = "{{.SecretKey}}";
        const enableSecret = {{.EnableSecret}};

        async function refreshData() {
            try {
                const res = await fetch('/health');
                const d = await res.json();
                const metrics = d.data.metrics;
                const deps = d.data.dependencies;

                document.getElementById('total-incoming').innerText = d.data.total_incoming || 0;
                document.getElementById('active-processors').innerText = d.data.active_processors || 0;
                document.getElementById('waiting-processors').innerText = d.data.waiting_processors || 0;
                document.getElementById('active-uploads').innerText = d.data.active_uploads || 0;
                document.getElementById('media-total').innerText = d.data.media_total_count;
                document.getElementById('success-count').innerText = "OK: " + d.data.successful_uploads;
                document.getElementById('fail-count').innerText = "FAIL: " + d.data.failed_uploads;
                document.getElementById('interrupted-count').innerText = d.data.interrupted_uploads || 0;
                
                const total = d.data.successful_uploads + d.data.failed_uploads;
                const rate = total > 0 ? ((d.data.successful_uploads / total) * 100).toFixed(1) : 0;
                document.getElementById('success-rate').innerText = rate + "%";

                document.getElementById('avg-camera').innerText = metrics.avg_camera_send_time;
                document.getElementById('avg-conversion').innerText = metrics.avg_conversion_time;
                document.getElementById('avg-s3').innerText = metrics.avg_s3_upload_time;
                document.getElementById('uptime').innerText = d.data.uptime;
                document.getElementById('last-at').innerText = d.data.last_processed_at || 'None';

                updateBadge('badge-s3', deps.s3_storage);
                updateBadge('badge-rmq', deps.rabbitmq);
            } catch (err) {}
        }

        function updateBadge(id, status) {
            const el = document.getElementById(id);
            if (status === 'ok') {
                el.innerText = 'ONLINE';
                el.className = 'text-[10px] font-black px-2 py-1 bg-green-500/10 text-green-400 rounded-md';
            } else if (status === 'not_configured') {
                el.innerText = 'N/A';
                el.className = 'text-[10px] font-black px-2 py-1 bg-slate-700/50 text-slate-500 rounded-md';
            } else {
                el.innerText = 'OFFLINE';
                el.className = 'text-[10px] font-black px-2 py-1 bg-red-500/10 text-red-500 rounded-md';
            }
        }

        document.getElementById('uploadForm').onsubmit = async (e) => {
            e.preventDefault();
            const statusDiv = document.getElementById('uploadStatus');
            const output = document.getElementById('responseOutput');
            statusDiv.classList.remove('hidden');
            output.innerText = "Transmitting...";
            
            const formData = new FormData(e.target);
            const ts = Date.now().toString();
            formData.append('timestamp', ts);

            if (enableSecret) {
                const fileName = e.target.filename.value || e.target.file.files[0].name;
                const hash = CryptoJS.MD5(fileName + ts + secretKey).toString();
                formData.append('sign', btoa(hash));
            }

            try {
                const res = await fetch('/upload', { method: 'POST', body: formData });
                const result = await res.json();
                output.innerText = JSON.stringify(result, null, 2);
                setTimeout(refreshData, 1000);
            } catch (err) {
                output.innerText = "Error: " + err.message;
            }
        }

        setInterval(refreshData, 3000);
        refreshData();
    </script>
</body>
</html>
`

	w.Header().Set("Content-Type", "text/html")

	tmpl := strings.ReplaceAll(html, "{{.SecretKey}}", h.cfg.SecretKey)
	if h.cfg.EnableSecret {
		tmpl = strings.ReplaceAll(tmpl, "{{.EnableSecret}}", "true")
	} else {
		tmpl = strings.ReplaceAll(tmpl, "{{.EnableSecret}}", "false")
	}

	w.Write([]byte(tmpl))
}

func (h *Handler) UploadHandler(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&h.totalIncoming, 1)
	atomic.AddInt64(&h.activeUploads, 1)
	startTime := time.Now()
	requestID := uuid.New().String()
	resultStatus := "nak" // Default is NAK (Negative Acknowledgement)

	defer func() {
		atomic.AddInt64(&h.activeUploads, -1)
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
		atomic.AddInt64(&h.failedUploads, 1)
		logger.Error("Failed to create multipart reader", "error", err)
		utils.WriteJSON(w, http.StatusBadRequest, utils.JSONResponse{Code: 400, Message: "Expected multipart/form-data"})
		return
	}

	var streamedTempPath string // Novo: guarda o path do arquivo já em disco
	defer func() {
		if streamedTempPath != "" {
			if _, err := os.Stat(streamedTempPath); err == nil {
				h.log.Warn("Cleaning up abandoned stream file", "path", streamedTempPath)
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
			atomic.AddInt64(&h.failedUploads, 1)
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
				atomic.AddInt64(&h.failedUploads, 1)
				logger.Error("Failed to create temporary file for streaming", "error", err)
				utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Internal server error"})
				return
			}
			streamedTempPath = tempFile.Name()

			// Usar um buffer maior para io.Copy para melhorar performance de rede/disco
			buffer := make([]byte, 1<<20) // 1MB buffer
			n, err := io.CopyBuffer(tempFile, part, buffer)
			if err != nil {
				atomic.AddInt64(&h.interruptedUploads, 1)
				tempFile.Close()
				os.Remove(streamedTempPath)
				streamedTempPath = "" // Reseta para o defer não tentar remover de novo
				logger.Error("Error saving file part stream", "bytes_read", n, "error", err)
				utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Failed to receive file content"})
				return
			}
			handlerSize = n
			tempFile.Close() // Fecha o arquivo pois já terminou a escrita do stream

			// Métrica: Tempo que a câmera levou para enviar o arquivo
			sendDuration := time.Since(startTime)
			atomic.AddInt64(&h.totalCameraSendTime, int64(sendDuration))
			atomic.AddInt64(&h.cameraSendCount, 1)

			continue // Já processamos o arquivo
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
		atomic.AddInt64(&h.failedUploads, 1)
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
		atomic.AddInt64(&h.failedUploads, 1) // Corrigindo: Incrementa falha para tirar da fila
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
			atomic.AddInt64(&h.failedUploads, 1)
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
			atomic.AddInt64(&h.failedUploads, 1)
			reqLogger.Error("Disaster Recovery mode is ON but BACKUP_VIDEO_PATH is not set.")
			utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Disaster recovery misconfigured"})
			return
		}
		savedPath = filepath.Join(h.cfg.BackupPath, finalFilename)
	} else {
		savedPath = filepath.Join(h.cfg.VideoPath, finalFilename)
	}

	// Define path de processamento isolado (fora da pasta final para mantê-la limpa)
	// Usamos um prefixo '.' para manter a pasta oculta se possível no mesmo nível da pasta de vídeos
	// Precisamos do nome da pasta pai (VideoPath ou BackupPath)
	uploadDir := h.cfg.VideoPath
	if h.cfg.DisasterRecoveryMode && h.cfg.BackupPath != "" {
		uploadDir = h.cfg.BackupPath
	}

	processingBase := filepath.Join(filepath.Dir(filepath.Clean(uploadDir)), ".processing_"+filepath.Base(filepath.Clean(uploadDir)))
	os.MkdirAll(processingBase, 0755)
	processingPath = filepath.Join(processingBase, finalFilename+"."+requestID+".tmp")

	// Move o arquivo já streamado para o caminho de processamento isolado
	if err := os.Rename(streamedTempPath, processingPath); err == nil {
		streamedTempPath = "" // Limpa para o defer não remover o arquivo movido
	} else {
		reqLogger.Warn("Failed to rename streamed file to processing dir, attempting copy", "error", err)
		if err := utils.CopyFile(streamedTempPath, processingPath); err != nil {
			atomic.AddInt64(&h.failedUploads, 1)
			reqLogger.Error("Processing copy failed", "error", err)
			utils.WriteJSON(w, http.StatusInternalServerError, utils.JSONResponse{Code: 500, Message: "Failed to save file"})
			return
		}
		os.Remove(streamedTempPath)
		streamedTempPath = ""
	}

	go h.processFile(processingPath, finalFilename, savedPath, reqLogger, h.cfg.EnableLocalStorage, startTime, fileSize)

	resultStatus = "ack" // Mark as ACK (Acknowledgement) for the summary log
	utils.WriteJSON(w, http.StatusOK, utils.JSONResponse{Code: 200, Message: "File upload success", Data: finalFilename})
}

func (h *Handler) processFile(path string, filename string, targetFinalPath string, logger *slog.Logger, isLocal bool, startTime time.Time, initialSize int64) {
	atomic.AddInt64(&h.waitingProcessors, 1)
	// Adquire semáforo para limitar processamento paralelo
	h.workerSemaphore <- struct{}{}
	atomic.AddInt64(&h.waitingProcessors, -1)
	atomic.AddInt64(&h.activeProcessors, 1)

	uploadPath := path
	uploadFilename := filename
	currentSize := initialSize
	ext := strings.ToLower(filepath.Ext(filename))

	defer func() {
		atomic.AddInt64(&h.activeProcessors, -1)
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

	// Conversão opcional TS -> MP4 antes do upload.
	if ext == ".ts" && h.cfg.EnableTsToMp4 {
		convStart := time.Now()
		convertedPath, err := processor.ConvertTSToMP4(uploadPath, logger)
		if err == nil {
			// Métrica: Sucesso na conversão
			atomic.AddInt64(&h.totalConversionTime, int64(time.Since(convStart)))
			atomic.AddInt64(&h.conversionCount, 1)

			// Captura o novo tamanho após conversão
			if stat, statErr := os.Stat(convertedPath); statErr == nil {
				currentSize = stat.Size()
				// Remove o arquivo temporário original se a conversão funcionou e temos o novo arquivo
				os.Remove(uploadPath)
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
	if ext == ".mp4" && h.cfg.EnableCompression {
		compStart := time.Now()
		compressedPath, err := processor.CompressWithFFmpeg(uploadPath, logger)
		if err == nil {
			// Somar tempo de compressão à métrica de conversão
			atomic.AddInt64(&h.totalConversionTime, int64(time.Since(compStart)))
			atomic.AddInt64(&h.conversionCount, 1)

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
		s3Start := time.Now()
		if err := h.storage.UploadFileToS3(uploadPath, uploadFilename, logger); err != nil {
			logger.Error("Failed to upload to S3", "error", err)
			atomic.AddInt64(&h.failedUploads, 1)
			return
		}
		// Métrica: Sucesso no upload S3
		atomic.AddInt64(&h.totalS3UploadTime, int64(time.Since(s3Start)))
		atomic.AddInt64(&h.s3UploadCount, 1)
		atomic.AddInt64(&h.successfulUploads, 1)
	} else {
		// Apenas incrementa sucesso se não houver upload externo habilitado
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
}

func (h *Handler) StartRecoveryTask() {
	logger := slog.With("task", "recovery")
	logger.Info("Starting recovery scan for orphaned files in .processing folders")

	scanDir := func(basePath string) {
		// Varre tanto o caminho novo (fora da pasta) quanto o antigo (dentro da pasta) para transição suave
		procDirs := []string{
			filepath.Join(basePath, ".processing"),
			filepath.Join(filepath.Dir(filepath.Clean(basePath)), ".processing_"+filepath.Base(filepath.Clean(basePath))),
		}

		for _, procDir := range procDirs {
			entries, err := os.ReadDir(procDir)
			if err != nil {
				if !os.IsNotExist(err) {
					logger.Error("Failed to read processing directory", "path", procDir, "error", err)
				}
				continue
			}

			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}

				filePath := filepath.Join(procDir, entry.Name())
				info, err := entry.Info()
				if err != nil {
					continue
				}

				// Ignora arquivos muito recentes (podem estar sendo escritos agora se o serviço acabou de subir)
				if time.Since(info.ModTime()) < 10*time.Second {
					continue
				}

				logger.Info("Recovering orphaned file", "file", entry.Name())

				// Tenta recuperar o nome original (remove .UUID.tmp do final)
				originalName := entry.Name()
				if strings.HasSuffix(originalName, ".tmp") {
					tempName := strings.TrimSuffix(originalName, ".tmp")
					lastDot := strings.LastIndex(tempName, ".")
					if lastDot != -1 {
						// Verifica se o que sobrou após o último ponto parece um UUID (36 chars)
						// Se sim, removemos. Se não, mantemos (pode ser arquivo comum .tmp)
						uuidPart := tempName[lastDot+1:]
						if len(uuidPart) == 36 || len(uuidPart) == 32 {
							originalName = tempName[:lastDot]
						}
					}
				}

				// Tenta inferir se é local baseado no path original (se estiver no VideoPath ou BackupPath)
				isLocal := strings.HasPrefix(filePath, h.cfg.VideoPath) || strings.HasPrefix(filePath, h.cfg.BackupPath)

				// Para arquivos recuperados, o targetFinalPath seria o diretório pai da pasta de destino final
				// No novo modelo, procDir está fora da pasta final, então usamos basePath diretamente
				targetFinalPath := filepath.Join(basePath, originalName)

				go h.processFile(filePath, originalName, targetFinalPath, logger, isLocal, time.Now(), info.Size())
			}
		}
	}

	if h.cfg.VideoPath != "" {
		scanDir(h.cfg.VideoPath)
	}
	if h.cfg.BackupPath != "" {
		scanDir(h.cfg.BackupPath)
	}
}
