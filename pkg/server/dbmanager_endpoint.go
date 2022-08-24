package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/crikke/oi/pkg/database"
	"github.com/crikke/oi/pkg/server/proto"
	"go.uber.org/zap/zapcore"
)

// db manager needs to hold a map of all databases

func (s *Server) CreateDatabase(ctx context.Context, in *proto.CreateDatabaseRequest) (*proto.CreateDatabaseResponse, error) {

	s.logger.Log(zapcore.InfoLevel, fmt.Sprintf("creating database '%s'", in.GetName()))
	if _, exist := s.databases[in.GetName()]; exist {
		return nil, fmt.Errorf("database with name '%s' already exist", in.GetName())
	}

	db, err := database.CreateDatabase(s.Configuration.Directory.Metadata, in.GetName())
	if err != nil {
		return nil, err
	}

	s.databases[db.Descriptor.Name] = db

	if err = db.Start(); err != nil {
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
