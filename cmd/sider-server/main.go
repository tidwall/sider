package main

import (
	"log"
	"os"
	"strings"

	"github.com/tidwall/sider/server"
)

func main() {
	if err := server.Start(&server.Options{
		Args: os.Args[1:], // pass the app args to the server
	}); err != nil {
		if !strings.HasPrefix(err.Error(), "options failure") &&
			!strings.HasPrefix(err.Error(), "config failure") {
			log.Print(err)
		}
	}
}
