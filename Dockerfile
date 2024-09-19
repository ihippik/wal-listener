# Dependencies Stage
FROM golang:1.23.1-alpine AS base
LABEL maintainer="Konstantin Makarov <hippik80@gmail.com>"

WORKDIR /listener
COPY go.mod go.sum ./
RUN go mod download

# Build Stage
FROM base AS build

WORKDIR /listener
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o app ./cmd/wal-listener

# Final Stage
FROM cgr.dev/chainguard/busybox:latest-glibc as prod

WORKDIR /app/

COPY --from=build /listener/app /app/

CMD /app/app