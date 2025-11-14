package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type JSONResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

const maxFilenameLength = 255

var (
	secretKey    = getEnv("SECRET_KEY", "jimidvr@123!443")
	enableSecret = getEnv("ENABLE_SECRET", "true") == "true"
	videoPath    = getEnv("LOCAL_VIDEO_PATH", "/data/upload")
	logFilePath  = "/app/dvr-upload/logs/server.log"
)

func main() {
	// setup logging
	if err := os.MkdirAll(filepath.Dir(logFilePath), 0755); err == nil {
		f, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			log.SetOutput(f)
		}
	}

	log.Printf("[UploadServer] Starting on :23010, saving files to %s", videoPath)
	os.MkdirAll(videoPath, 0755)

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/ping", pingHandler)
	log.Fatal(http.ListenAndServe(":23010", nil))
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, JSONResponse{Code: 400, Message: "Invalid form data"})
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, JSONResponse{Code: 400, Message: "File is required"})
		return
	}
	defer file.Close()

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

	log.Printf("[UploadServer] Received orig=%s computed=%s timestamp=%s sign=%s err=%v", handler.Filename, finalFilename, timestamp, sign, buildErr)

	if len(finalFilename) > maxFilenameLength {
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
			log.Printf("[UploadServer] Invalid signature: %s (expected %s) for filename=%s", sign, expected, finalFilename)
			writeJSON(w, http.StatusBadRequest, JSONResponse{Code: 400, Message: "Signature error"})
			return
		}
	}

	dstPath := filepath.Join(videoPath, finalFilename)
	dst, err := os.Create(dstPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSONResponse{Code: 500, Message: "Failed to save file"})
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		writeJSON(w, http.StatusInternalServerError, JSONResponse{Code: 500, Message: "Write failed"})
		return
	}

	log.Printf("[UploadServer] Upload success: %s", finalFilename)
	writeJSON(w, http.StatusOK, JSONResponse{Code: 200, Message: "File upload success", Data: finalFilename})
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
