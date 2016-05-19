all: 
	@ go build -o sider-server cmd/sider-server/*.go
clean:
	rm -f sider-server
install: all
	cp sider-server /usr/local/bin
uninstall: 
	rm -f /usr/local/bin/sider-server
