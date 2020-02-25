package main

// TODO: Format!

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/reflection"

	// "github.com/golang/protobuf/proto"

	pb "github.com/gnossen/kvd/kvd"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type keyValueStore struct {
	m  map[string]string
	mu sync.RWMutex
}

func newKeyValueStore() *keyValueStore {
	var store keyValueStore
	store.m = make(map[string]string)
	return &store
}

func (s *keyValueStore) GetRecord(ctx context.Context, request *pb.GetRecordRequest) (*pb.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var value string
	var exists bool
	if value, exists = s.m[request.Name]; !exists {
		return &pb.Record{}, status.Errorf(codes.NotFound, fmt.Sprintf("Record at key '%s' not found.", request.Name))
	}
	return &pb.Record{Name: request.Name, Value: value}, nil
}

func (s *keyValueStore) CreateRecord(ctx context.Context, request *pb.CreateRecordRequest) (*pb.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[request.Record.Name]; exists {
		return &pb.Record{}, status.Errorf(codes.InvalidArgument, fmt.Sprintf("Record at key '%s already exists.", request.Record.Name))
	}
	s.m[request.Record.Name] = request.Record.Value
	return &pb.Record{Name: request.Record.Name, Value: request.Record.Value}, nil
}

func (s *keyValueStore) UpdateRecord(ctx context.Context, request *pb.UpdateRecordRequest) (*pb.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[request.Record.Name]; !exists {
		return &pb.Record{}, status.Errorf(codes.NotFound, fmt.Sprintf("Record at key '%s' not found.", request.Record.Name))
	}
	s.m[request.Record.Name] = request.Record.Value
	return &pb.Record{Name: request.Record.Name, Value: request.Record.Value}, nil
}

func (s *keyValueStore) WatchRecord(request *pb.WatchRecordRequest, stream pb.KeyValueStore_WatchRecordServer) error {
	return nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	//.TODO: Reflection.
	grpcServer := grpc.NewServer()
	store := newKeyValueStore()
	pb.RegisterKeyValueStoreServer(grpcServer, store)
	reflection.Register(grpcServer)
	grpcServer.Serve(lis)
}
