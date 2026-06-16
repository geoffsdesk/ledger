package server

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// leaseManager implements TTL leases. The apiserver attaches a lease to
// short-lived objects (most importantly Events, ~1h TTL) so they self-expire.
// Leases live in memory; a reaper deletes the attached keys when they lapse,
// which the backend records as ordinary delete revisions (so watches observe
// the expiry naturally).
type leaseManager struct {
	be  backend.Backend
	mu  sync.Mutex
	tab map[int64]*leaseEntry
	rng *rand.Rand
}

type leaseEntry struct {
	id     int64
	ttl    int64 // granted seconds
	expiry time.Time
}

func newLeaseManager(be backend.Backend) *leaseManager {
	return &leaseManager{
		be:  be,
		tab: map[int64]*leaseEntry{},
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (m *leaseManager) start(ctx context.Context) { go m.reap(ctx) }

func (m *leaseManager) reap(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			m.mu.Lock()
			var expired []int64
			for id, e := range m.tab {
				if now.After(e.expiry) {
					expired = append(expired, id)
					delete(m.tab, id)
				}
			}
			m.mu.Unlock()
			for _, id := range expired {
				if _, err := m.be.DeleteByLease(ctx, id); err != nil {
					// Re-arm so we retry on the next tick.
					m.mu.Lock()
					m.tab[id] = &leaseEntry{id: id, ttl: 1, expiry: now.Add(time.Second)}
					m.mu.Unlock()
				}
			}
		}
	}
}

func (m *leaseManager) grant(id, ttl int64) int64 {
	if ttl <= 0 {
		ttl = 1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if id == 0 {
		for {
			id = int64(m.rng.Uint64() & 0x7fffffffffffffff)
			if id != 0 {
				if _, exists := m.tab[id]; !exists {
					break
				}
			}
		}
	}
	m.tab[id] = &leaseEntry{id: id, ttl: ttl, expiry: time.Now().Add(time.Duration(ttl) * time.Second)}
	return id
}

func (m *leaseManager) renew(id int64) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.tab[id]
	if !ok {
		return 0, false
	}
	e.expiry = time.Now().Add(time.Duration(e.ttl) * time.Second)
	return e.ttl, true
}

func (m *leaseManager) revoke(ctx context.Context, id int64) {
	m.mu.Lock()
	delete(m.tab, id)
	m.mu.Unlock()
	_, _ = m.be.DeleteByLease(ctx, id)
}

func (m *leaseManager) ttl(id int64) (granted, remaining int64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.tab[id]
	if !ok {
		return 0, 0, false
	}
	rem := int64(time.Until(e.expiry).Seconds())
	if rem < 0 {
		rem = 0
	}
	return e.ttl, rem, true
}

func (m *leaseManager) list() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]int64, 0, len(m.tab))
	for id := range m.tab {
		ids = append(ids, id)
	}
	return ids
}

func (s *Server) LeaseGrant(ctx context.Context, r *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	id := s.leases.grant(r.ID, r.TTL)
	cur, _ := s.be.CurrentRevision(ctx)
	ttl := r.TTL
	if ttl <= 0 {
		ttl = 1
	}
	return &etcdserverpb.LeaseGrantResponse{Header: s.header(cur), ID: id, TTL: ttl}, nil
}

func (s *Server) LeaseRevoke(ctx context.Context, r *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	s.leases.revoke(ctx, r.ID)
	cur, _ := s.be.CurrentRevision(ctx)
	return &etcdserverpb.LeaseRevokeResponse{Header: s.header(cur)}, nil
}

func (s *Server) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		ttl, ok := s.leases.renew(req.ID)
		cur, _ := s.be.CurrentRevision(stream.Context())
		resp := &etcdserverpb.LeaseKeepAliveResponse{Header: s.header(cur), ID: req.ID, TTL: ttl}
		if !ok {
			resp.TTL = 0 // lease gone: a zero TTL tells the client it expired
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *Server) LeaseTimeToLive(ctx context.Context, r *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	cur, _ := s.be.CurrentRevision(ctx)
	granted, remaining, ok := s.leases.ttl(r.ID)
	resp := &etcdserverpb.LeaseTimeToLiveResponse{Header: s.header(cur), ID: r.ID, GrantedTTL: granted, TTL: remaining}
	if !ok {
		resp.TTL = -1 // -1: lease does not exist
	}
	return resp, nil
}

func (s *Server) LeaseLeases(ctx context.Context, r *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	cur, _ := s.be.CurrentRevision(ctx)
	resp := &etcdserverpb.LeaseLeasesResponse{Header: s.header(cur)}
	for _, id := range s.leases.list() {
		resp.Leases = append(resp.Leases, &etcdserverpb.LeaseStatus{ID: id})
	}
	return resp, nil
}
