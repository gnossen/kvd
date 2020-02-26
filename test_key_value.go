package kvd

import (
	"testing"
	"github.com/gnossen/kvd/client"
	"github.com/gnossen/kvd/server"
	pb "github.com/gnossen/kvd/kvd"

	"github.com/golang/protobuf/proto"
)

func TestUnary(t *testing.T) {
	server, lis := server.NewServer()
	go server.Serve(lis)
	defer server.Stop()
	defer lis.Close()
	conn, err := grpc.Dial(*serverAddr, []grpc.DialOption{grpc.WithInsecure()}...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	cl := pb.NewKeyValueStoreClient(conn)
	record := client.Create(cl, "foo", "oof")
	expected = pb.Record{Name: "foo", Value: "oof"}
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
	record = client.Get(cl)
	if !proto.Equal(record, &expected) {
		t.Fatalf("Expected '%v', got '%v'", expected, *record)
	}
}
