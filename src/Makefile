include Makefile.defs

all: deps server

deps:
	dep ensure

server:
	go build -v -o $(NAME) 

clean:
	rm -f $(NAME)

test:
	go test -v
	rm bolt.db

