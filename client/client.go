package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"

	pb "github.com/gnossen/kvd/kvd"
)

var (
	serverAddr = flag.String("server_addr", "localhost:50051", "The server address in the format of host:port")
)

func Create(client pb.KeyValueStoreClient, name string, value string) *pb.Record {
	request := pb.CreateRecordRequest{Record: &pb.Record{Name: name, Value: value}}
	var record *pb.Record
	var err error
	if record, err = client.CreateRecord(context.Background(), &request); err != nil {
		log.Fatalf("Creation failed: %v", err)
	}
	return record
}

func Update(client pb.KeyValueStoreClient, name string, value string) *pb.Record {
	request := pb.UpdateRecordRequest{Record: &pb.Record{Name: name, Value: value}}
	var record *pb.Record
	var err error
	if record, err = client.UpdateRecord(context.Background(), &request); err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	return record
}

func Get(client pb.KeyValueStoreClient, name string) *pb.Record {
	request := pb.GetRecordRequest{Name: name}
	var record *pb.Record
	var err error
	if record, err = client.GetRecord(context.Background(), &request); err != nil {
		log.Fatalf("Get operation failed: %v", err)
	}
	return record
}

func Watch(client pb.KeyValueStoreClient, name string, watchCount int) chan *pb.Record{
	c := make(chan *pb.Record)
	go func() {
		request := pb.WatchRecordRequest{Name: name}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.WatchRecord(ctx, &request)
		if err != nil {
			log.Fatalf("Failed to watch key '%s': %v", name, err)
		}
		var record *pb.Record
		recordCount := 0
		for {
			if watchCount >= 0 && recordCount == watchCount {
				break
			}
			if record, err = stream.Recv(); err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Encountered error: %v", err)
			}
			c <- record
			recordCount += 1
		}
		close(c)
	}()
	return c
}

func PrintRecord(record *pb.Record) {
	fmt.Printf("'%s': '%s'\n", record.Name, record.Value)
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
	watchName := watchCmd.String("name", "", "The name to watch.")

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
		PrintRecord(Create(client, *createName, *createValue))
	case "update":
		updateCmd.Parse(flag.Args()[1:])
		PrintRecord(Update(client, *updateName, *updateValue))
	case "get":
		getCmd.Parse(flag.Args()[1:])
		PrintRecord(Get(client, *getName))
	case "watch":
		watchCmd.Parse(flag.Args()[1:])
		for record := range Watch(client, *watchName, -1) {
			PrintRecord(record)
		}
	default:
		log.Fatalf("Unsupported command '%s'", flag.Args()[1])
	}
}
