// Separate module so the end-to-end test's clientv3 dependency does not perturb
// the main module's dependency graph (clientv3's newer releases pull a gRPC that
// conflicts with the Spanner client's grpc-gcp balancer).
module github.com/geoffsdesk/ledger/test/integration

go 1.21

require (
	github.com/geoffsdesk/ledger v0.0.0
	go.etcd.io/etcd/client/v3 v3.5.13
	google.golang.org/grpc v1.64.0
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	go.etcd.io/etcd/api/v3 v3.5.13 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.13 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240506185236-b8a5c65736ae // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240513163218-0867130af1f8 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
)

replace github.com/geoffsdesk/ledger => ../..
