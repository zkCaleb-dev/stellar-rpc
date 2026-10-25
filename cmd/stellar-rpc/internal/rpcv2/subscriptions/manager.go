// Package subscriptions implements server-push delivery of contract events
// (PROTOTYPE): a fan-out registry fed by the ingestion loop's per-ledger
// commit signal, serving each subscriber through the same getEvents read path
// the JSON-RPC endpoint uses, cursor for cursor.
//
// Shape: push for freshness, pull for correctness. The ingestion loop only
// WAKES the manager (Notify — one atomic store and a non-blocking channel
// send, so the hot path never blocks on a subscriber); a single worker
// goroutine then advances every subscription by polling getEvents in-process
// from that subscription's cursor. Because delivery is cursor-driven over the
// durable stores, a subscriber can never miss an event between subscribing
// and the first wake, and the cursor it receives with every push is the same
// cursor getEvents hands a poller — on disconnect, a client resumes gaplessly
// over plain HTTP getEvents and re-subscribes.
package subscriptions

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// GetEvents serves one v1 getEvents request in-process. Production binds the
// eventsapi handler over a fresh read view per call; tests bind fakes.
type GetEvents func(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)

// Push is one delivery to a subscriber: the events of one getEvents page, or
// a terminal error (Err set), after which the subscription is removed.
type Push struct {
	SubscriptionID string               `json:"subscriptionId"`
	LatestLedger   uint32               `json:"latestLedger,omitempty"`
	Cursor         string               `json:"cursor,omitempty"`
	Events         []protocol.EventInfo `json:"events,omitempty"`
	Err            string               `json:"error,omitempty"`
}

// Sink receives a subscription's pushes. Deliver must not block: false
// reports the subscriber cannot keep up, and the manager ends the
// subscription (the slow-consumer policy; the transport owns closing itself).
type Sink interface {
	Deliver(Push) bool
}

// maxWindowsPerPass bounds how many getEvents windows one subscription may
// consume in a single worker pass, so a deep historical catch-up cannot
// starve the other subscriptions; the pass re-wakes itself to continue.
const maxWindowsPerPass = 100

// defaultBatchLimit is the per-poll page size, clamped to the bound source's
// own max at Bind time.
const defaultBatchLimit = 256

type subscriber struct {
	id          string
	filters     []protocol.EventFilter
	sink        Sink
	startLedger uint32           // first poll's startLedger; 0 = anchor at the wake's ledger
	cursor      *protocol.Cursor // resume point after the first poll
}

// Manager is the subscription registry and fan-out worker. Zero subscribers
// cost one atomic store per committed ledger.
type Manager struct {
	logger *supportlog.Entry

	mu   sync.Mutex
	subs map[string]*subscriber

	src   GetEvents
	limit uint

	latest atomic.Uint32 // highest fully-queryable ledger (Notify)
	nextID atomic.Uint64
	wake   chan struct{} // coalesced wake signal, capacity 1
}

func NewManager(logger *supportlog.Entry) *Manager {
	return &Manager{
		logger: logger,
		subs:   map[string]*subscriber{},
		limit:  defaultBatchLimit,
		wake:   make(chan struct{}, 1),
	}
}

// Bind hands the manager its read path, clamping the poll page size to the
// source's own per-request max. Call before Run.
func (m *Manager) Bind(src GetEvents, maxLimit uint) {
	m.src = src
	if maxLimit > 0 && maxLimit < m.limit {
		m.limit = maxLimit
	}
}

// Notify records that ledger seq is fully queryable and wakes the worker.
// Called from the ingestion loop's commit path: one atomic store and one
// non-blocking send, never more.
func (m *Manager) Notify(seq uint32) {
	m.latest.Store(seq)
	m.Wake()
}

// Wake nudges the worker without advancing the latest ledger (used by
// Subscribe so a new subscription with a historical start drains promptly,
// and by the pass cap to continue a long catch-up).
func (m *Manager) Wake() {
	select {
	case m.wake <- struct{}{}:
	default: // a wake is already pending; passes coalesce
	}
}

// Subscribe registers sink under a fresh subscription id. startLedger 0 means
// "events from now on": anchored past the latest committed ledger when one
// exists, else at the first wake's ledger. A positive startLedger replays
// from there (subject to the source's retention window — out-of-range
// surfaces as the subscription's terminal Err push on the first pass).
func (m *Manager) Subscribe(filters []protocol.EventFilter, startLedger uint32, sink Sink) string {
	s := &subscriber{
		id:          "sub-" + strconv.FormatUint(m.nextID.Add(1), 10),
		filters:     filters,
		sink:        sink,
		startLedger: startLedger,
	}
	if startLedger == 0 {
		if latest := m.latest.Load(); latest > 0 {
			// The window-end cursor at the latest ledger: getEvents resumes at
			// latest+1, so the subscriber sees only ledgers committed after
			// this call, the v1 caught-up-poller arithmetic exactly.
			c := protocol.MaxCursor
			c.Ledger = latest
			s.cursor = &c
		}
		// latest == 0: no commit yet this run; the first poll anchors at the
		// wake's own ledger, which is the first one committed after this call.
	}
	m.mu.Lock()
	m.subs[s.id] = s
	m.mu.Unlock()
	m.Wake()
	return s.id
}

// Unsubscribe removes id; unknown ids are a no-op.
func (m *Manager) Unsubscribe(id string) {
	m.mu.Lock()
	delete(m.subs, id)
	m.mu.Unlock()
}

// Run is the fan-out worker: one pass over the subscriptions per coalesced
// wake, until ctx cancels. Single goroutine by design — it is the only writer
// of the per-subscription cursors.
func (m *Manager) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			m.pass(ctx)
		}
	}
}

func (m *Manager) pass(ctx context.Context) {
	if m.src == nil {
		return
	}
	m.mu.Lock()
	snapshot := make([]*subscriber, 0, len(m.subs))
	for _, s := range m.subs {
		snapshot = append(snapshot, s)
	}
	m.mu.Unlock()

	for _, s := range snapshot {
		if ctx.Err() != nil {
			return
		}
		m.advance(ctx, s)
	}
}

// advance polls getEvents from s's cursor until s is caught up to the latest
// committed ledger, the pass cap trips, or the subscription ends (slow sink,
// or a terminal source error pushed as Err).
func (m *Manager) advance(ctx context.Context, s *subscriber) {
	for windows := 0; windows < maxWindowsPerPass; windows++ {
		req := protocol.GetEventsRequest{
			Filters:    s.filters,
			Pagination: &protocol.PaginationOptions{Limit: m.limit},
		}
		switch {
		case s.cursor != nil:
			c := *s.cursor
			req.Pagination.Cursor = &c
		case s.startLedger > 0:
			req.StartLedger = s.startLedger
		default:
			// Anchor "from now on" at the wake's ledger, inclusive: the
			// subscriber was registered before this ledger committed.
			anchor := m.latest.Load()
			if anchor == 0 {
				return // nothing committed yet; next wake anchors
			}
			req.StartLedger = anchor
		}

		resp, err := m.src(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Terminal for the subscription, not the connection: the sink
			// hears why (e.g. a startLedger outside retention), then the
			// subscription is removed. The client decides whether to
			// re-subscribe shallower.
			s.sink.Deliver(Push{SubscriptionID: s.id, Err: err.Error()})
			m.Unsubscribe(s.id)
			return
		}

		next, perr := protocol.ParseCursor(resp.Cursor)
		if perr != nil {
			m.logger.WithError(perr).WithField("cursor", resp.Cursor).
				Error("subscriptions: unparseable getEvents cursor")
			m.Unsubscribe(s.id)
			return
		}
		s.cursor = &next

		if len(resp.Events) > 0 {
			ok := s.sink.Deliver(Push{
				SubscriptionID: s.id,
				LatestLedger:   resp.LatestLedger,
				Cursor:         resp.Cursor,
				Events:         resp.Events,
			})
			if !ok {
				m.Unsubscribe(s.id) // slow consumer; the sink owns its close
				return
			}
		}

		if uint(len(resp.Events)) == m.limit {
			continue // full page: more within this window
		}
		if next.Ledger >= m.latest.Load() {
			return // short page at the tip: caught up
		}
		// Short page below the tip: the scan window ended mid-history
		// (getEvents bounds each request to one scan window); keep walking.
	}
	m.Wake() // pass cap: yield to the other subscriptions, then continue
}

// SubscriberCount reports the live subscription count (tests, metrics).
func (m *Manager) SubscriberCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subs)
}
