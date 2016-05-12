all: 
	go build -o tile38-server cmd/tile38-server/*.go
clean:
	rm -f tile38-server
	rm -f tile38-cli
install: all
	cp tile38-server /usr/local/bin
	cp tile38-cli /usr/local/bin
uninstall: 
	rm -f /usr/local/bin/tile38-server
	rm -f /usr/local/bin/tile38-cli
