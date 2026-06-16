package spanner

import (
	"context"
	"errors"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
)

// ChangeStreamDDL creates the change stream the notifier tails. Apply it
// alongside the base schema when running with --watch-source=changestream:
//
//	CREATE CHANGE STREAM kine_stream FOR kine
const ChangeStreamDDL = "CREATE CHANGE STREAM kine_stream FOR kine"

var errChangeStreamUnimplemented = errors.New(
	"spanner change-streams notifier is specified in docs/adr but not yet implemented; " +
		"run with the default --watch-source=poll")

// ChangeStreamNotifier is an EXPERIMENTAL backend.Notifier that tails a Spanner
// change stream on the kine table instead of polling, which removes poll
// latency and the steady poll read-load at scale.
//
// It is wired behind --watch-source=changestream but intentionally not yet
// implemented: the Cloud Spanner emulator cannot exercise change streams, so we
// will not ship an unvalidated reader as a watch path. The full design —
// data-change-record decoding, commit-timestamp ordering (which matches
// revision order because the revision counter advances in commit order), the
// child-partition DAG, and checkpointing — is specified in the ADR for
// implementation and validation against real Cloud Spanner.
type ChangeStreamNotifier struct {
	be *Backend
}

// NewChangeStreamNotifier returns the (experimental) change-streams notifier.
func NewChangeStreamNotifier(be *Backend) *ChangeStreamNotifier {
	return &ChangeStreamNotifier{be: be}
}

func (c *ChangeStreamNotifier) Poll(context.Context, int64) ([]*backend.Event, error) {
	return nil, errChangeStreamUnimplemented
}

func (c *ChangeStreamNotifier) Interval() time.Duration { return 10 * time.Millisecond }

var _ backend.Notifier = (*ChangeStreamNotifier)(nil)
