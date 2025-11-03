# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src

# Cache dependencies first
COPY go.mod go.sum ./
ENV GOTOOLCHAIN=auto
RUN go mod download

# Copy the entire repository to build the MDNS example binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/graft-node ./examples/simple-cluster-mdns

FROM alpine:3.19
WORKDIR /app

COPY --from=build /out/graft-node /app/server

ENTRYPOINT ["/app/server"]
