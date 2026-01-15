package utils

import (
"crypto/md5"
"encoding/base64"
"fmt"
)

func GenerateSign(filename, timestamp, secret string) string {
sum := md5.Sum([]byte(filename + timestamp + secret))
return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%x", sum)))
}
