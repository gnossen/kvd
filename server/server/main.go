package main

import (
	"flag"
	"github.com/gnossen/kvd/server"
)

var (
	port = flag.Int("port", 50051, "The server port")
)


func main() {
	flag.Parse()
	server, lis := server.NewServer(*port)
	defer server.Stop()
	defer lis.Close()
	server.Serve(lis)
}
