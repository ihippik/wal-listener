.PHONE: build buildx lint test check
build:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t ihippik/wal-listener .

buildx:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t ihippik/wal-listener:v2.9.1 .

lint:
	golangci-lint run --config=.golangci.yml ./...

test:
	go test ./...

check: lint test