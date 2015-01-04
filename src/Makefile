include Makefile.defs

all: deps server

deps:
	CGO_CFLAGS="-I/usr/local//Cellar/leveldb/1.18/include/" CGO_LDFLAGS="-L/usr/local//Cellar/leveldb/1.18/lib" go get -v

server:
	CGO_CFLAGS="-I/usr/local//Cellar/leveldb/1.18/include/" CGO_LDFLAGS="-L/usr/local//Cellar/leveldb/1.18/lib" go build -v -o $(NAME) -ldflags "-X main.VERSION $(VERSION)"

clean:
	rm -f $(NAME)

test:
	go test -v
	rm bolt.db

.PHONY: server
