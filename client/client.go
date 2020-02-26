package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	pb "github.com/gnossen/kvd/kvd"
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		request := pb.WatchRecordRequest{Name: name}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.WatchRecord(ctx, &request)
		wg.Done()
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
	wg.Wait()
	return c
}

func PrintRecord(record *pb.Record) {
	fmt.Printf("'%s': '%s'\n", record.Name, record.Value)
}

