FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o static-discovery ./examples/discovery/static/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/static-discovery .
RUN mkdir -p data
EXPOSE 8080 9090
CMD ["./static-discovery"]