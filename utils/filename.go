package utils

import (
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
)

const MaxFilenameLength = 255

func BuildStandardFilename(r *http.Request, fh *multipart.FileHeader) (string, error) {
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
	if pattern == "event" || (imei != "" && typ != "" && channel != "") {
		// Validate IMEI numeric
		if imei != "" && !regexp.MustCompile(`^[0-9]{8,20}$`).MatchString(imei) {
			return "", errors.New("invalid imei")
		}
		if typ != "" && typ != "I" && typ != "F" {
			return "", errors.New("invalid type (expect I or F)")
		}
		if channel != "" && !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(channel) {
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
	if pattern == "snapshot" || (imei != "" && rawHex != "") {
		if imei != "" && !regexp.MustCompile(`^[0-9]{8,20}$`).MatchString(imei) {
			return "", errors.New("invalid imei")
		}
		if rawHex != "" && !regexp.MustCompile(`^[0-9A-Fa-f]+$`).MatchString(rawHex) {
			return "", errors.New("invalid raw hex block")
		}
		if channel != "" && !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(channel) {
			return "", errors.New("invalid channel")
		}
		if index != "" && !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(index) {
			return "", errors.New("invalid index")
		}
		return fmt.Sprintf("%s_%s_%s_%s%s", imei, rawHex, channel, leftPad(index, 2), ext), nil
	}

	return "", errors.New("insufficient data for standardized filename")
}

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

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
