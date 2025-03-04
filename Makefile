run-redis:
	go run main.go benchmark --database redis --host localhost --port 6379

add-license-notice:
	./add_license_notice.sh

lint:
	gofmt -w .
	golangci-lint run ./...
