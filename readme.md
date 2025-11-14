# 📦 DVR Upload Server (Local)

Um servidor simples em **Go** para receber **uploads de arquivos** (ex: vídeos de DVR) via `multipart/form-data`.

O servidor valida a assinatura do arquivo, salva localmente e registra logs detalhados de cada operação.  
Ideal para ambientes IoT / edge onde não há necessidade de enviar diretamente para S3, OSS, Azure, etc.

---

## 🚀 Funcionalidades

- 📤 Upload de arquivos via `POST /upload`
- 🔒 Validação opcional de assinatura MD5 + Base64
- 🧾 Logs completos (arquivo + console)
- 💾 Armazenamento local configurável
- ❤️ Endpoint `/ping` para health checks
- 🐳 Compatível com Docker e Docker Compose

---

## 🛠️ Variáveis de Ambiente

| Variável | Descrição | Valor Padrão |
|-----------|------------|---------------|
| `ENABLE_SECRET` | Ativa/desativa verificação de assinatura | `true` |
| `SECRET_KEY` | Chave usada para gerar/validar a assinatura | `jimidvr@123!443` |
| `LOCAL_VIDEO_PATH` | Caminho onde os arquivos serão salvos | `/data/upload` |

---

## 🧾 Formato da Requisição de Upload

### Endpoint
