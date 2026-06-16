package spanner

import (
	"context"
	"fmt"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DDL is the Spanner schema for the shim. The whole keyspace lives in one table
// (kine), append-only: every Put/Delete is a new row whose primary key `id` is
// the global revision. kine_meta holds two single-row counters: the revision
// allocator and the compaction watermark.
//
// Note the deliberate single-row counter (kine_meta where k='revision'). It is
// the serialization point that gives us a strictly monotonic, gap-free revision
// in commit order — the property the apiserver depends on. It is also the
// primary write hotspot; see the README for how this is sharded at scale.
var DDL = []string{
	`CREATE TABLE kine (
		id              INT64       NOT NULL,
		name            STRING(MAX) NOT NULL,
		created         BOOL        NOT NULL,
		deleted         BOOL        NOT NULL,
		create_revision INT64       NOT NULL,
		prev_revision   INT64       NOT NULL,
		lease           INT64       NOT NULL,
		value           BYTES(MAX)  NOT NULL,
		old_value       BYTES(MAX)  NOT NULL,
	) PRIMARY KEY (id)`,

	`CREATE TABLE kine_meta (
		k STRING(MAX) NOT NULL,
		v INT64        NOT NULL,
	) PRIMARY KEY (k)`,

	// Latest-version-per-key lookups and range scans.
	`CREATE INDEX kine_name_id ON kine(name, id DESC)`,

	// Lease reaping: find live keys attached to an expiring lease.
	`CREATE INDEX kine_lease ON kine(lease, name)`,
}

// Init creates the instance (if missing), the database, and the schema. It is a
// convenience for the Spanner emulator and local dev; in production the schema
// is applied via your normal DDL migration pipeline (deploy/schema.sql).
//
// Requires SPANNER_EMULATOR_HOST to be set when targeting the emulator.
func Init(ctx context.Context, projectID, instanceID, databaseID string) error {
	iac, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("instance admin client: %w", err)
	}
	defer iac.Close()

	op, err := iac.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + projectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create instance: %w", err)
	}
	if err == nil {
		if _, err := op.Wait(ctx); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("await create instance: %w", err)
		}
	}

	dac, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("database admin client: %w", err)
	}
	defer dac.Close()

	dbOp, err := dac.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
		ExtraStatements: DDL,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("create database: %w", err)
	}
	if _, err := dbOp.Wait(ctx); err != nil {
		return fmt.Errorf("await create database: %w", err)
	}
	return nil
}
