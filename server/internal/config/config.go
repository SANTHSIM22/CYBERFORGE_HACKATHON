// Package config reads typed settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// MustString panics with a clear message when a required variable is missing.
func MustString(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required environment variable %s is not set", key))
	}
	return v
}

func Int(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func Bool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func Duration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// SecretRef resolves an endpoint_secret_ref from the registry. Development uses
// "env:NAME"; a real secret manager adapter can be added without schema changes.
func SecretRef(ref string) (string, error) {
	const prefix = "env:"
	if len(ref) > len(prefix) && ref[:len(prefix)] == prefix {
		v := os.Getenv(ref[len(prefix):])
		if v == "" {
			return "", fmt.Errorf("secret ref %q: environment variable is empty", ref)
		}
		return v, nil
	}
	return "", fmt.Errorf("unsupported secret ref %q", ref)
}
