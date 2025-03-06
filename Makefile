run-dicedb:
	go run main.go benchmark --database dicedb --host localhost --port 7379  --num-requests 1000000 --num-clients 4 --emit-metrics-sink prometheus

run-redis:
	go run main.go benchmark --database redis --host localhost --port 6379

run-null:
	go run main.go benchmark --database null

add-license-notice:
	./add_license_notice.sh

lint:
	gofmt -w .
	golangci-lint run ./...
