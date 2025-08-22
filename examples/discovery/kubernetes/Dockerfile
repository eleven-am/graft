FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o k8s-discovery ./examples/discovery/kubernetes/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates curl
WORKDIR /app
COPY --from=builder /app/k8s-discovery .
RUN mkdir -p /data
EXPOSE 8080 9090
USER 1000:1000
CMD ["./k8s-discovery"]