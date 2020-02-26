package kvd

import (
	"testing"
	"github.com/gnossen/kvd/client"
	"github.com/gnossen/kvd/server"
)

func TestGet(t *testing.T) {
	server, lis := server.NewServer()
	go server.Serve(lis)
	defer server.Stop()
	defer lis.Close()
	conn, err := grpc.Dial(*serverAddr, []grpc.DialOption{grpc.WithInsecure()}...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewKeyValueStoreClient(conn)
}
