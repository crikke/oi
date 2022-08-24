package server

import (
	"context"
	"errors"

	"github.com/crikke/oi/pkg/server/proto"
	"google.golang.org/grpc"
)

func (s *Server) Put(ctx context.Context, in *proto.PutRequest, opts ...grpc.CallOption) (*proto.ResponseStatus, error) {

	db, ok := s.databases[in.GetDatabase()]

	if !ok {
		return nil, errors.New("database not found")
	}

	err := db.Put(ctx, []byte(in.GetKey()), in.GetValue())

	if err != nil {
		return nil, err
	}

	return &proto.ResponseStatus{}, nil
}

func (s *Server) Get(ctx context.Context, in *proto.GetRequest, opts ...grpc.CallOption) (*proto.GetResponse, error) {
	panic("not implemented") // TODO: Implement
}
