//go:generate protoc -I../kvd ../kvd/key_value.proto --go_out=plugins=grpc:../kvd

package main

// TODO: Format!

import (
	list "container/list"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/gnossen/kvd/kvd"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type keyValueStore struct {
	m        map[string]string
	mu       sync.RWMutex
	watchers map[string]*list.List // List[*chan string]
}

func newKeyValueStore() *keyValueStore {
	var store keyValueStore
	store.m = make(map[string]string)
	store.watchers = make(map[string]*list.List)
	return &store
}

func (s *keyValueStore) upsertLocked(key string, value string) {
	s.m[key] = value
	if watchers, exists := s.watchers[key]; exists {
		for elem := watchers.Front(); elem != nil; elem = elem.Next() {
			fmt.Printf("Notifying watcher for key '%s'\n", key)
			watcher := elem.Value.(*chan string)
			*watcher <- value
		}
	}
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
	s.upsertLocked(request.Record.Name, request.Record.Value)
	return &pb.Record{Name: request.Record.Name, Value: request.Record.Value}, nil
}

func (s *keyValueStore) UpdateRecord(ctx context.Context, request *pb.UpdateRecordRequest) (*pb.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[request.Record.Name]; !exists {
		return &pb.Record{}, status.Errorf(codes.NotFound, fmt.Sprintf("Record at key '%s' not found.", request.Record.Name))
	}
	s.upsertLocked(request.Record.Name, request.Record.Value)
	return &pb.Record{Name: request.Record.Name, Value: request.Record.Value}, nil
}

func (s *keyValueStore) addWatcher(key string) (*chan string, *list.Element) {
	fmt.Printf("Adding watcher for key '%s'\n", key)
	c := make(chan string)
	s.mu.Lock()
	defer s.mu.Unlock()
	var exists bool
	if _, exists = s.watchers[key]; !exists {
		s.watchers[key] = list.New()
	}
	elem := s.watchers[key].PushBack(&c)
	return &c, elem
}

func (s *keyValueStore) removeWatcher(key string, elem *list.Element) {
	fmt.Printf("Removing watcher for key '%s'\n", key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watchers[key].Remove(elem)
}

func (s *keyValueStore) WatchRecord(request *pb.WatchRecordRequest, stream pb.KeyValueStore_WatchRecordServer) error {
	c, elem := s.addWatcher(request.Name)
	defer s.removeWatcher(request.Name, elem)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case value := <-*c:
			stream.Send(&pb.Record{Name: request.Name, Value: value})
		}
	}
	return nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	store := newKeyValueStore()
	pb.RegisterKeyValueStoreServer(grpcServer, store)
	reflection.Register(grpcServer)
	grpcServer.Serve(lis)
}
