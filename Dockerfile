FROM alpine:3.16

MAINTAINER Konstantin Makarov <hippik80@gmail.com>
RUN adduser -D developer
WORKDIR /app
COPY wal-listener .
USER developer

ENTRYPOINT ["./wal-listener"]