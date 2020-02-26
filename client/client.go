package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"google.golang.org/grpc"

	pb "github.com/gnossen/kvd/kvd"
)

var (
	serverAddr = flag.String("server_addr", "localhost:50051", "The server address in the format of host:port")
)

func create(client pb.KeyValueStoreClient, name string, value string) {
	request := pb.CreateRecordRequest{Record: &pb.Record{Name: name, Value: value}}
	if _, err := client.CreateRecord(context.Background(), &request); err != nil {
		log.Fatalf("Creation failed: %v", err)
	}
	fmt.Printf("Created key '%s' with value '%s'\n", name, value)
}

func update(client pb.KeyValueStoreClient, name string, value string) {
	request := pb.UpdateRecordRequest{Record: &pb.Record{Name: name, Value: value}}
	if _, err := client.UpdateRecord(context.Background(), &request); err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	fmt.Printf("Update key '%s' to value '%s'\n", name, value)
}

func get(client pb.KeyValueStoreClient, name string) {
	request := pb.GetRecordRequest{Name: name}
	var record *pb.Record
	var err error
	if record, err = client.GetRecord(context.Background(), &request); err != nil {
		log.Fatalf("Get operation failed: %v", err)
	}
	fmt.Printf("Key '%s' has value '%s'\n", record.Name, record.Value)
}

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
	// watchName := watchCmd.String("name", "", "The name to watch.")

	flag.Parse()
	conn, err := grpc.Dial(*serverAddr, []grpc.DialOption{grpc.WithInsecure()}...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewKeyValueStoreClient(conn)

	if len(flag.Args()) < 2 {
		log.Fatalf("Expected a command.")
	}

	switch flag.Args()[0] {
	case "create":
		createCmd.Parse(flag.Args()[1:])
		create(client, *createName, *createValue)
	case "update":
		updateCmd.Parse(flag.Args()[1:])
		update(client, *updateName, *updateValue)
	case "get":
		getCmd.Parse(flag.Args()[1:])
		get(client, *getName)
	case "watch":
		watchCmd.Parse(flag.Args()[1:])
	default:
		log.Fatalf("Unsupported command '%s'", flag.Args()[1])
	}
}
