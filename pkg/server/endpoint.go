package server

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/crikke/oi/pkg/database"
	"github.com/crikke/oi/pkg/server/proto"
	"github.com/google/uuid"
)

func (s *Server) CreateDatabase(ctx context.Context, in *proto.CreateDatabaseRequest) (*proto.CreateDatabaseResponse, error) {

	entries, err := s.loadDatabaseDescriptors()
	if err != nil {
		return nil, fmt.Errorf("[CreateDatabase] fatal: %w", err)
	}
	for _, descriptor := range entries {
		if descriptor.Name == in.GetName() {
			return nil, errors.New(fmt.Sprintf("Database with name '%s' already exist", in.GetName()))
		}
	}

	d := database.Descriptor{
		Name: in.GetName(),
		UUID: uuid.New(),
	}

	f, err := os.OpenFile(fmt.Sprintf("%s%s", d.UUID.String(), database.DescriptorPrefix), os.O_CREATE|os.O_APPEND, 660)
	defer f.Close()
	database.EncodeDescriptor(f, d)
	return nil, nil
}

func (s *Server) StopDatabase(ctx context.Context, in *proto.StopDatabaseRequest) (*proto.StopDatabaseResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (s *Server) StartDatabase(ctx context.Context, in *proto.StartDatabaseRequest) (*proto.StartDatabaseResponse, error) {
	panic("not implemented") // TODO: Implement
}
