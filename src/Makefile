include Makefile.defs

all: deps server

deps:
	go get -v

server:
	go build -v -o $(NAME) -ldflags "-X main.VERSION $(VERSION)"

clean:
	rm -f $(NAME)

.PHONY: server
