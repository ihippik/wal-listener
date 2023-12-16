FROM golang:1.21 AS build-env
LABEL maintainer="Konstantin Makarov <hippik80@gmail.com>"

ADD . /listener
WORKDIR /listener

RUN go build -buildmode=pie -trimpath -ldflags='-s -w -buildid' -o app ./cmd/wal-listener

FROM cgr.dev/chainguard/busybox:latest-glibc

WORKDIR /run/app

COPY --from=build-env /listener/app /app/

CMD /app/app