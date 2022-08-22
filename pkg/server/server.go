package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/crikke/oi/pkg/database"
	pb "github.com/crikke/oi/pkg/server/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

// The engine is the main component which orchestrates all other components
// It handles scheduling of SSTable merges & writes, Memtree flushes.
// It also exposes operations for reading & writing data
type ServerConfiguration struct {
	Port int

	Directory struct {
		Metadata string
	}

	Database database.Configuration
}

type Server struct {
	Configuration ServerConfiguration
	databases     []*database.Database
	logger        *zap.Logger

	pb.UnimplementedDatabaseManagerServiceServer
}

func NewServer(cfg ServerConfiguration) (*Server, error) {

	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	logger.Log(zapcore.InfoLevel, "Starting Server...")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		panic(err)
	}
	s := &Server{logger: logger, Configuration: cfg}
	grpcServer := grpc.NewServer()
	pb.RegisterDatabaseManagerServiceServer(grpcServer, s)
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}

	return s, nil
}

func (s Server) Start() {

	descriptors, err := s.loadDatabaseDescriptors()
	if err != nil {
		panic(err)
	}
	s.databases = make([]*database.Database, 0)

	var wg sync.WaitGroup

	wg.Add(len(descriptors))
	for _, descriptor := range descriptors {

		defer wg.Done()
		db, err := database.Init(descriptor, s.Configuration.Database)
		if err != nil {
			panic(err)
		}

		// once the database is initialized it is considered to be running and should accept requests
		s.databases = append(s.databases, db)
	}

	wg.Wait()
}

func (s Server) loadDatabaseDescriptors() ([]database.Descriptor, error) {
	ensureDirExists(s.Configuration.Directory.Metadata)

	entries, err := os.ReadDir(s.Configuration.Directory.Metadata)

	if err != nil {
		panic(err)
	}

	md := make([]database.Descriptor, 0)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), database.DescriptorPrefix) {

			f, err := os.Open(entry.Name())
			defer f.Close()
			if err != nil {
				log.Fatal(err)
			}
			m, err := database.DecodeDescriptor(f)

			if err != nil {
				log.Fatal(err)
			}
			md = append(md, m)
		}
	}
	return md, nil
}

func ensureDirExists(dir string) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0664)
	}
}
