clean:
	rm bin/*

build-server:
	go mod tidy && go build -o bin/server cmd/server/main.go

build-cli:
	go mod tidy && go build -o bin/cli cmd/cli/main.go