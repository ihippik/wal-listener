FROM golang:1.21-alpine AS build-env
LABEL maintainer="Konstantin Makarov <hippik80@gmail.com>"

WORKDIR /listener
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o app ./cmd/wal-listener

FROM cgr.dev/chainguard/busybox:latest-glibc

WORKDIR /app/

COPY --from=build-env /listener/app /app/

CMD /app/wal-listener