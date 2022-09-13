FROM alpine:3.16

MAINTAINER Konstantin Makarov <hippik80@gmail.com>
RUN adduser -D dev
WORKDIR /app
COPY wal-listener .
USER dev

ENTRYPOINT ["./wal-listener"]