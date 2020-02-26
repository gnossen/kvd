package main

import (
	"flag"
	"github.com/gnossen/kvd/client"
	"log"
	pb "github.com/gnossen/kvd/kvd"
	"google.golang.org/grpc"
)

var (
	serverAddr = flag.String("server_addr", "localhost:50051", "The server address in the format of host:port")
)

func main() {
	createCmd := flag.NewFlagSet("create", flag.ExitOnError)
	createName := createCmd.String("name", "", "The name to create.")
	createValue := createCmd.String("value", "", "The value with which to create.")

	updateCmd := flag.NewFlagSet("update", flag.ExitOnError)
	updateName := updateCmd.String("name", "", "The name to update.")
	updateValue := updateCmd.String("value", "", "The value to update.")

	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	getName := getCmd.String("name", "", "The name to get.")

	watchCmd := flag.NewFlagSet("watch", flag.ExitOnError)
	watchName := watchCmd.String("name", "", "The name to watch.")

	flag.Parse()
	conn, err := grpc.Dial(*serverAddr, []grpc.DialOption{grpc.WithInsecure()}...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	cl := pb.NewKeyValueStoreClient(conn)

	if len(flag.Args()) < 2 {
		log.Fatalf("Expected a command.")
	}

	switch flag.Args()[0] {
	case "create":
		createCmd.Parse(flag.Args()[1:])
		client.PrintRecord(client.Create(cl, *createName, *createValue))
	case "update":
		updateCmd.Parse(flag.Args()[1:])
		client.PrintRecord(client.Update(cl, *updateName, *updateValue))
	case "get":
		getCmd.Parse(flag.Args()[1:])
		client.PrintRecord(client.Get(cl, *getName))
	case "watch":
		watchCmd.Parse(flag.Args()[1:])
		for record := range client.Watch(cl, *watchName, -1) {
			client.PrintRecord(record)
		}
	default:
		log.Fatalf("Unsupported command '%s'", flag.Args()[1])
	}
}
