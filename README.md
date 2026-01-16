# 📦 DVR Upload Server

Um servidor robusto em **Go** para receber **uploads de arquivos** (ex: vídeos de DVR) via `multipart/form-data`.

O servidor valida assinaturas de arquivo, salva localmente e/ou na nuvem (OCI S3-compatible), realiza conversão de formato (TS→MP4) e registra logs detalhados.  
Ideal para ambientes IoT / edge com suporte a **Disaster Recovery Mode**.

---

## 🚀 Funcionalidades

- 📤 Upload de arquivos via `POST /upload`
- 🔒 Validação opcional de assinatura MD5 + Base64
- 💾 Armazenamento local configurável
- ☁️ Suporte a OCI Object Storage (S3-compatible)
- 🎬 Conversão automática TS→MP4 (via FFmpeg)
- 🔄 Modo Disaster Recovery com backup automático
- 🧾 Logs completos (arquivo JSON + console)
- ❤️ Endpoint `/ping` para health checks
- 🐳 Compatível com Docker e Docker Compose

---

## 🛠️ Variáveis de Ambiente

| Variável | Descrição | Valor Padrão |
|-----------|------------|---------------|
| `ENABLE_SECRET` | Ativa/desativa verificação de assinatura | `true` |
| `SECRET_KEY` | Chave para gerar/validar assinatura | `jimidvr@123!443` |
| `LOCAL_VIDEO_PATH` | Caminho de armazenamento local | `/data/upload` |
| `BACKUP_VIDEO_PATH` | Caminho para backup local | `/data/dvr-upload-backup` |
| `ENABLE_LOCAL_STORAGE` | Ativa armazenamento local | `true` |
| `ENABLE_TS_TO_MP4` | Ativa conversão TS→MP4 | `true` |
| `DISASTER_RECOVERY_MODE` | Ativa modo Disaster Recovery | `false` |
| `OCI_BUCKET_MEDIA` | Nome do bucket OCI | (vazio) |
| `OCI_REGION` | Região do OCI | `sa-saopaulo-1` |
| `OCI_ENDPOINT` | Endpoint do OCI | (vazio) |
| `OCI_ACCESS_KEY_ID` | Chave de acesso OCI | (vazio) |
| `OCI_SECRET_ACCESS_KEY` | Chave secreta OCI | (vazio) |
| `OCI_USE_PATH_STYLE_ENDPOINT` | Usar path-style no OCI | `true` |

---

## 🧾 Formato da Requisição de Upload

### Endpoint

```http
POST /upload HTTP/1.1
Content-Type: multipart/form-data
```

### Parâmetros

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `file` | File | **Obrigatório** - Arquivo a ser enviado |
| `filename` | String | Nome customizado do arquivo (opcional) |
| `timestamp` | String | Timestamp da requisição (obrigatório se `ENABLE_SECRET=true`) |
| `sign` | String | Assinatura MD5+Base64 (obrigatório se `ENABLE_SECRET=true`) |

### Exemplo de requisição

```bash
# Gerar assinatura (Python)
python3 -c "
import hashlib
import base64
import sys

filename = 'meu_video.ts'
timestamp = '1705334400'
secret = 'jimidvr@123!443'

msg = f'{filename}{timestamp}'.encode()
key = secret.encode()
sig = base64.b64encode(
    hashlib.md5(msg + key).digest()
).decode()
print(sig)
"

# Enviar arquivo
curl -X POST http://localhost:23010/upload \
  -F "file=@meu_video.ts" \
  -F "filename=meu_video.ts" \
  -F "timestamp=1705334400" \
  -F "sign=<assinatura_gerada>"
```

### Resposta de sucesso

```json
{
  "code": 200,
  "message": "File uploaded successfully",
  "data": {
    "file_id": "550e8400-e29b-41d4-a716-446655440000",
    "filename": "meu_video.ts",
    "size": 1024000,
    "saved_path": "/data/upload/meu_video.ts"
  }
}
```

### Resposta de erro

```json
{
  "code": 400,
  "message": "Invalid signature"
}
```

---

## 🚀 Como Executar

### Localmente

```bash
# Dependências
go mod download

# Build
go build -o dvr-upload .

# Executar
./dvr-upload
```

O servidor iniciará em `http://localhost:23010`

### Com Docker

```bash
# Build
docker build -t dvr-upload:latest .

# Run
docker run -p 23010:23010 \
  -v /data/upload:/data/upload \
  -v /app/dvr-upload/logs:/app/dvr-upload/logs \
  -e ENABLE_SECRET=true \
  -e SECRET_KEY=jimidvr@123!443 \
  dvr-upload:latest
```

### Com Docker Compose

```bash
docker-compose up -d
```

---

## 🏗️ Arquitetura

```
├── main.go           # Ponto de entrada
├── config/           # Carregamento de configurações
├── handlers/         # Handlers HTTP
├── storage/          # Serviço de armazenamento (local + OCI)
├── processor/        # Processamento de vídeos (FFmpeg)
└── utils/            # Utilitários (criptografia, resposta JSON, etc)
```

---

## 🛡️ Health Check

```bash
curl http://localhost:23010/ping
# Resposta: {"code":200,"message":"ok"}
```

---

## 📊 Logs

Logs são salvos em formato JSON em `/app/dvr-upload/logs/server.log` e também exibidos no console.

Exemplo de log:
```json
{
  "level": "info",
  "msg": "Upload request received",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "remote_addr": "192.168.1.100:54321",
  "method": "POST",
  "uri": "/upload"
}
```

---

## 🔄 Disaster Recovery Mode

Quando `DISASTER_RECOVERY_MODE=true`, o servidor cria backup automático dos arquivos em `BACKUP_VIDEO_PATH`.

---

## 🐳 Dockerfile

- **Base**: Alpine Linux com FFmpeg
- **Build**: Compilação em múltiplos estágios
- **Porta**: 23010
- **Volumes**: `/data/upload`, `/app/dvr-upload/logs`

---

## 📦 Dependências

- `github.com/sirupsen/logrus` - Logging
- `github.com/google/uuid` - Geração de UUIDs
- `github.com/aws/aws-sdk-go-v2` - Cliente OCI S3
- `ffmpeg` - Conversão de vídeos (em container)

---

## 📝 Licença

Desenvolvido para JimiIoTHub
