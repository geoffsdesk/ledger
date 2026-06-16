// Command ledger is an etcd v3 gRPC server backed by Google Cloud
// Spanner. It runs on the Kubernetes control-plane VM beside kube-apiserver and
// is pointed at via --etcd-servers, standing in for a real etcd.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"github.com/geoffsdesk/ledger/pkg/backend/memory"
	spannerbe "github.com/geoffsdesk/ledger/pkg/backend/spanner"
	"github.com/geoffsdesk/ledger/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

func main() {
	var (
		listen       = flag.String("listen", "127.0.0.1:2379", "address to serve the etcd v3 API on")
		backendName  = flag.String("backend", "spanner", "storage backend: spanner | memory")
		dbPath       = flag.String("spanner-database", "", "Spanner database: projects/P/instances/I/databases/D")
		initSchema   = flag.Bool("init", false, "create the instance/database/schema before serving (emulator/dev)")
		pollInterval = flag.Duration("watch-poll-interval", 100*time.Millisecond, "how often to poll the backend for watch events")
		watchSource  = flag.String("watch-source", "poll", "watch event source: poll | changestream (changestream is experimental; spanner only)")
		certFile     = flag.String("cert-file", "", "TLS server certificate (enables TLS)")
		keyFile      = flag.String("key-file", "", "TLS server private key")
		caFile       = flag.String("trusted-ca-file", "", "CA bundle to verify client certs (enables mTLS)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	be, err := buildBackend(ctx, *backendName, *dbPath, *initSchema)
	if err != nil {
		log.Fatalf("backend: %v", err)
	}
	defer be.Close()

	var srv *server.Server
	if *watchSource == "changestream" {
		sb, ok := be.(*spannerbe.Backend)
		if !ok {
			log.Fatalf("--watch-source=changestream requires --backend=spanner")
		}
		srv = server.NewWithNotifier(be, spannerbe.NewChangeStreamNotifier(sb))
	} else {
		srv = server.New(be, *pollInterval)
	}
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(16 << 20),
		grpc.MaxSendMsgSize(16 << 20),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	if *certFile != "" && *keyFile != "" {
		creds, err := loadTLS(*certFile, *keyFile, *caFile)
		if err != nil {
			log.Fatalf("tls: %v", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	gs := grpc.NewServer(opts...)
	srv.Register(gs)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	go func() {
		<-ctx.Done()
		log.Printf("shutting down")
		gs.GracefulStop()
	}()

	scheme := "http"
	if *certFile != "" {
		scheme = "https"
	}
	log.Printf("ledger serving etcd v3 API on %s://%s (backend=%s)", scheme, *listen, *backendName)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func buildBackend(ctx context.Context, name, dbPath string, initSchema bool) (backend.Backend, error) {
	switch name {
	case "memory":
		return memory.New(), nil
	case "spanner":
		if dbPath == "" {
			return nil, fmt.Errorf("--spanner-database is required for the spanner backend")
		}
		if initSchema {
			project, instance, database, err := parseDBPath(dbPath)
			if err != nil {
				return nil, err
			}
			log.Printf("initialising schema at %s", dbPath)
			if err := spannerbe.Init(ctx, project, instance, database); err != nil {
				return nil, fmt.Errorf("init: %w", err)
			}
		}
		return spannerbe.New(ctx, dbPath)
	default:
		return nil, fmt.Errorf("unknown backend %q", name)
	}
}

func parseDBPath(p string) (project, instance, database string, err error) {
	parts := strings.Split(p, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "instances" || parts[4] != "databases" {
		return "", "", "", fmt.Errorf("invalid --spanner-database %q (want projects/P/instances/I/databases/D)", p)
	}
	return parts[1], parts[3], parts[5], nil
}

func loadTLS(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certs parsed from %s", caFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(cfg), nil
}
