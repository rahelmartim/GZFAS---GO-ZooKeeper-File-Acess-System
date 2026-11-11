SITE := site
PORT ?= 8080

.PHONY: test test-zookeeper web serve server zookeeper clean

## test: vet and run the tests with the race detector
test:
	go vet ./...
	go test -race ./...

## test-zookeeper: run the tests on ZooKeeper at localhost:2181 too (it deletes /GZFAS)
test-zookeeper:
	ZOOKEEPER_SERVERS=localhost:2181 go test -race -count=1 ./...

## web: build the browser demo (WebAssembly) into site/
web:
	rm -rf $(SITE) && mkdir -p $(SITE)
	cp web/index.html web/style.css web/app.js $(SITE)/
	cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" $(SITE)/
	GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o $(SITE)/gzfas.wasm ./cmd/gzfas-wasm

## serve: build the browser demo and serve it on http://localhost:$(PORT)
serve: web
	python3 -m http.server $(PORT) --directory $(SITE)

## zookeeper: start ZooKeeper 3.9 in Docker on localhost:2181
zookeeper:
	docker run --rm -d --name gzfas-zookeeper -p 2181:2181 zookeeper:3.9

## server: run the dashboard on a real ZooKeeper at localhost:2181
server:
	go run ./cmd/gzfas-server -zk localhost:2181

clean:
	rm -rf $(SITE) gzfas-data
