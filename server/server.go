//go:generate protoc -I../kvd ../kvd/key_value.proto --go_out=plugins=grpc:../kvd

package server

import (
	list "container/list"
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/gnossen/kvd/kvd"
)

type kvStore struct {
	m        map[string]string
	mu       sync.RWMutex
	watchers map[string]*list.List // List[*chan string]
	ctx      context.Context
}

func newKeyValueStore() *kvStore {
	var store kvStore
	store.m = make(map[string]string)
	store.watchers = make(map[string]*list.List)
	return &store
}

func (s *kvStore) upsertLocked(key string, value string) {
	s.m[key] = value
	if watchers, exists := s.watchers[key]; exists {
		for elem := watchers.Front(); elem != nil; elem = elem.Next() {
			watcher := elem.Value.(*chan string)
			*watcher <- value
		}
	}
}

func peerString(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	} else {
		return ""
	}
}

func (s *kvStore) GetRecord(ctx context.Context, request *pb.GetRecordRequest) (*pb.Record, error) {
	log.Printf("%s: Get '%s'\n", peerString(ctx), request.Name)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var value string
	var exists bool
	if value, exists = s.m[request.Name]; !exists {
		return &pb.Record{},
			status.Errorf(codes.NotFound,
				fmt.Sprintf("Record at key '%s' not found.",
					request.Name))
	}
	return &pb.Record{Name: request.Name, Value: value}, nil
}

func (s *kvStore) CreateRecord(ctx context.Context, request *pb.CreateRecordRequest) (*pb.Record, error) {
	log.Printf("%s: Create '%s': '%s'\n", peerString(ctx), request.Record.Name, request.Record.Value)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[request.Record.Name]; exists {
		return &pb.Record{},
			status.Errorf(codes.InvalidArgument,
				fmt.Sprintf("Record at key '%s already exists.",
					request.Record.Name))
	}
	s.upsertLocked(request.Record.Name, request.Record.Value)
	return &pb.Record{Name: request.Record.Name, Value: request.Record.Value}, nil
}

func (s *kvStore) UpdateRecord(ctx context.Context, request *pb.UpdateRecordRequest) (*pb.Record, error) {
	log.Printf("%s: Update '%s': '%s'\n", peerString(ctx), request.Record.Name, request.Record.Value)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[request.Record.Name]; !exists {
		return &pb.Record{},
			status.Errorf(codes.NotFound,
				fmt.Sprintf("Record at key '%s' not found.",
					request.Record.Name))
	}
	s.upsertLocked(request.Record.Name, request.Record.Value)
	return &pb.Record{Name: request.Record.Name, Value: request.Record.Value}, nil
}

func (s *kvStore) addWatcher(key string) (*chan string, *list.Element) {
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

func (s *kvStore) removeWatcher(key string, elem *list.Element) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watchers[key].Remove(elem)
}

func (s *kvStore) WatchRecord(request *pb.WatchRecordRequest,
	stream pb.KeyValueStore_WatchRecordServer) error {
	log.Printf("%s: Start Watch '%s'\n", peerString(stream.Context()), request.Name)
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
	log.Printf("%s: End Watch '%s'\n", peerString(stream.Context()), request.Name)
	return nil
}

func NewServer(port int) (*grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	store := newKeyValueStore()
	pb.RegisterKeyValueStoreServer(grpcServer, store)
	reflection.Register(grpcServer)
	return grpcServer, lis
}
