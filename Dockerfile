FROM node:20-alpine AS frontend
WORKDIR /ui
COPY proxy/ui/package*.json ./
RUN npm ci
COPY proxy/ui/ .
RUN npm run build

FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum ./
COPY go.mod go.sum ./
COPY proxy/go.mod proxy/go.sum ./proxy/
RUN cd proxy && go mod download
COPY . .
COPY --from=frontend /ui/dist ./proxy/ui/dist
RUN cd proxy && CGO_ENABLED=0 go build -o /usr/local/bin/promclick-proxy ./cmd/proxy/ \
 && CGO_ENABLED=0 go build -o /usr/local/bin/promclick-writer ./cmd/writer/ \
 && CGO_ENABLED=0 go build -o /usr/local/bin/promclick-downsampler ./cmd/downsampler/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /usr/local/bin/promclick-* /usr/local/bin/
WORKDIR /app
ENTRYPOINT ["promclick-proxy"]
CMD ["--config", "proxy.yaml"]
