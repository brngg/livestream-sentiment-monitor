package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func HashIdentity(salt, username string) string {
	value := strings.ToLower(strings.TrimSpace(username))
	if value == "" {
		value = "unknown"
	}
	sum := sha256.Sum256([]byte(salt + ":" + value))
	return hex.EncodeToString(sum[:])
}
