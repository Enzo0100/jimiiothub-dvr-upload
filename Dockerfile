FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
# go.mod já existe no repositório; baixar dependências e compilar
RUN go mod download && go build -o dvr-upload .

FROM alpine:latest
RUN apk add --no-cache ffmpeg
WORKDIR /app/dvr-upload
COPY --from=builder /app/dvr-upload .
RUN mkdir -p /app/dvr-upload/logs /data/upload
EXPOSE 23010
CMD ["./dvr-upload"]
