.PHONE: build buildx
build:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t ihippik/wal-listener .

buildx:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t ihippik/wal-listener:v2.4.3 .