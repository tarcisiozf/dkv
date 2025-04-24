# --- Build stage ---
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .

RUN go mod tidy
RUN go build -o app cmd/server/main.go

# --- Runtime stage ---
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/app .

ENTRYPOINT ["./app"]