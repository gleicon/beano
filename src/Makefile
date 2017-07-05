include Makefile.defs

all: deps server

deps:
	go get -v

server:
	go build -v -o $(NAME) 

clean:
	rm -f $(NAME)

test:
	go test -v
	rm bolt.db

