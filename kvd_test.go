package kvd

import (
	"testing"
	"google.golang.org/grpc"
	"github.com/gnossen/kvd/client"
	"github.com/gnossen/kvd/server"
	pb "github.com/gnossen/kvd/kvd"

	"github.com/golang/protobuf/proto"
)

func TestUnary(t *testing.T) {
	server, lis := server.NewServer(1234)
	go server.Serve(lis)
	defer server.Stop()
	defer lis.Close()
	conn, err := grpc.Dial("localhost:1234", []grpc.DialOption{grpc.WithInsecure()}...)
	if err != nil {
		t.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	cl := pb.NewKeyValueStoreClient(conn)
	record := client.Create(cl, "foo", "oof")
	expected := pb.Record{Name: "foo", Value: "oof"}
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
	record = client.Get(cl, "foo")
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
	record = client.Update(cl, "foo", "bigoof")
	expected = pb.Record{Name: "foo", Value: "bigoof"}
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
	record = client.Get(cl, "foo")
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
}

func TestWatch(t *testing.T) {
	server, lis := server.NewServer(1234)
	go server.Serve(lis)
	defer server.Stop()
	defer lis.Close()
	conn, err := grpc.Dial("localhost:1234", []grpc.DialOption{grpc.WithInsecure()}...)
	if err != nil {
		t.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	cl := pb.NewKeyValueStoreClient(conn)
	c := client.Watch(cl, "foo", 3)
	go func() {
		client.Create(cl, "foo", "5")
		client.Update(cl, "foo", "6")
		client.Update(cl, "foo", "7")
	}()
	expected := pb.Record{Name: "foo", Value: "5"}
	record := <-c
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
	expected.Value = "6"
	record = <-c
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
	expected.Value = "7"
	record = <-c
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
}

