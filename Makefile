BINARY_NAME=benbroo
BUILD_DIR=build

.PHONY: build run test clean tidy

build: tidy
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/server/

run: build
	./$(BUILD_DIR)/$(BINARY_NAME)

test:
	go test -v -race ./...

tidy:
	go mod tidy

clean:
	rm -rf $(BUILD_DIR)

sql:
	mysql -u root -p < sql/schema.sql
