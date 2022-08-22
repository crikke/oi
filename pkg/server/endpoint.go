package server

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/crikke/oi/pkg/database"
	"github.com/crikke/oi/pkg/server/proto"
	"github.com/google/uuid"
	"go.uber.org/zap/zapcore"
)

func (s *Server) CreateDatabase(ctx context.Context, in *proto.CreateDatabaseRequest) (*proto.CreateDatabaseResponse, error) {

	s.logger.Log(zapcore.InfoLevel, fmt.Sprintf("creating database '%s'", in.GetName()))
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
	if err := database.EncodeDescriptor(f, d); err != nil {
		return nil, err
	}

	return &proto.CreateDatabaseResponse{
		Code: &proto.ResponseStatus{
			Code:            0,
			ResponseMessage: "ok",
		},
	}, nil
}

func (s *Server) StopDatabase(ctx context.Context, in *proto.StopDatabaseRequest) (*proto.StopDatabaseResponse, error) {

	db, ok := s.databases[in.GetName()]

	if !ok {
		return nil, errors.New("database not found")
	}

	s.logger.Log(zapcore.InfoLevel, fmt.Sprintf("stopping database '%s'", in.GetName()))
	if err := db.Stop(); err != nil {
		return nil, fmt.Errorf("[StopDatabase] error stopping database: %w", err)
	}

	// if db successfully stopped, update descriptor

	return &proto.StopDatabaseResponse{
			Code: &proto.ResponseStatus{
				Code:            0,
				ResponseMessage: "ok",
			},
		},
		nil
}

func (s *Server) StartDatabase(ctx context.Context, in *proto.StartDatabaseRequest) (*proto.StartDatabaseResponse, error) {
	panic("not implemented") // TODO: Implement
}
