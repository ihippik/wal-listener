.PHONE: build
build:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t ihippik/wal-listener .