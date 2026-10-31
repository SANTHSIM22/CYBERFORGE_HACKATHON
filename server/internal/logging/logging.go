// Package logging configures structured JSON logs for every service.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

func Setup(service string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	var h slog.Handler
	if os.Getenv("LOG_FORMAT") == "text" {
		h = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	} else {
		h = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	}
	l := slog.New(h).With("service", service)
	slog.SetDefault(l)
	return l
}
