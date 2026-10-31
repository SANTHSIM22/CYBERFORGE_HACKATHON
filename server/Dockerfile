# syntax=docker/dockerfile:1.7
# One image, four binaries: api, aggregator, demo, tpctl. Compose picks the command.
FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata wget && adduser -D -u 10001 tp
COPY --from=build /out/ /usr/local/bin/
USER tp
ENV LOG_FORMAT=json
