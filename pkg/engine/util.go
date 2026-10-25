package engine

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// nowUTC returns the current UTC time. It is a tiny wrapper so callers
// read clearly and the clock can be swapped later if needed.
func nowUTC() time.Time {
	return time.Now().UTC()
}

// newID returns a random 128-bit hex identifier.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// cloneMap returns a shallow copy of src, or nil if src is nil.
func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}

	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
