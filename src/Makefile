include Makefile.defs

all: deps server

deps:
	CGO_CFLAGS="-I/usr/local//Cellar/leveldb/1.18/include/ -I/usr/local/Cellar/snappy/1.1.1/include/" CGO_LDFLAGS="-L/usr/local//Cellar/leveldb/1.18/lib -L/usr/local//Cellar/snappy/1.1.1/lib" go get -v

server:
	CGO_CFLAGS="-I/usr/local//Cellar/leveldb/1.18/include/ -I/usr/local/Cellar/snappy/1.1.1/include/" CGO_LDFLAGS="-L/usr/local//Cellar/leveldb/1.18/lib -L/usr/local//Cellar/snappy/1.1.1/lib" go build -v -race -o $(NAME) -ldflags "-X main.VERSION $(VERSION)"

clean:
	rm -f $(NAME)

test:
	go test -v
	rm bolt.db

.PHONY: server
