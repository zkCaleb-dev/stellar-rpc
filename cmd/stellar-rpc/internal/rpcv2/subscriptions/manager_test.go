package subscriptions

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// fakeStore serves getEvents over an in-memory, cursor-ordered event list,
// reproducing the two v1 cursor behaviors the manager leans on: a cursor
// resumes AFTER the cursor (the +1 event arithmetic), and a short page hands
// back the window-end cursor at the latest ledger.
type fakeStore struct {
	mu     sync.Mutex
	events []protocol.EventInfo // ordered by ID (IDs compare lexicographically)
	latest uint32
	calls  int
}

func (f *fakeStore) add(ledger uint32, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range n {
		id := protocol.Cursor{Ledger: ledger, Tx: 1, Op: 0, Event: uint32(i)} //nolint:gosec // test data
		f.events = append(f.events, protocol.EventInfo{
			ID:         id.String(),
			Ledger:     int32(ledger), //nolint:gosec // test data
			ContractID: "C" + fmt.Sprint(ledger),
			EventType:  "contract",
		})
	}
	if ledger > f.latest {
		f.latest = ledger
	}
}

func (f *fakeStore) source() GetEvents {
	return func(_ context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls++
		var startID string
		if req.Pagination != nil && req.Pagination.Cursor != nil {
			c := *req.Pagination.Cursor
			c.Event++
			startID = c.String()
		} else {
			startID = protocol.Cursor{Ledger: req.StartLedger}.String()
		}
		limit := 0
		if req.Pagination != nil {
			limit = int(req.Pagination.Limit) //nolint:gosec // test sizes
		}
		var out []protocol.EventInfo
		for _, e := range f.events {
			if e.ID >= startID {
				out = append(out, e)
			}
			if limit > 0 && len(out) == limit {
				break
			}
		}
		resp := protocol.GetEventsResponse{Events: out, LatestLedger: f.latest}
		if limit > 0 && len(out) == limit {
			resp.Cursor = out[len(out)-1].ID
		} else {
			we := protocol.MaxCursor
			we.Ledger = f.latest
			resp.Cursor = we.String()
		}
		return resp, nil
	}
}

// chanSink collects pushes; reject flips the slow-consumer answer.
type chanSink struct {
	ch     chan Push
	reject bool
}

func (s *chanSink) Deliver(p Push) bool {
	if s.reject {
		return false
	}
	s.ch <- p
	return true
}

func newTestManager(t *testing.T, store *fakeStore) *Manager {
	t.Helper()
	m := NewManager(silentLogger())
	m.Bind(store.source(), 0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go m.Run(ctx)
	return m
}

func recv(t *testing.T, ch chan Push) Push {
	t.Helper()
	select {
	case p := <-ch:
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a push")
		return Push{}
	}
}

func expectNoPush(t *testing.T, ch chan Push) {
	t.Helper()
	select {
	case p := <-ch:
		t.Fatalf("unexpected push: %+v", p)
	case <-time.After(150 * time.Millisecond):
	}
}

// A subscriber registered before the first commit receives that first
// ledger's events (the anchor is the wake's own ledger, inclusive), and later
// wakes deliver only what came after the cursor.
func TestSubscribeBeforeFirstCommit(t *testing.T) {
	store := &fakeStore{}
	m := newTestManager(t, store)
	sink := &chanSink{ch: make(chan Push, 16)}

	id := m.Subscribe(nil, 0, sink)
	expectNoPush(t, sink.ch) // nothing committed yet

	store.add(5, 3)
	m.Notify(5)
	p := recv(t, sink.ch)
	assert.Equal(t, id, p.SubscriptionID)
	assert.Len(t, p.Events, 3)
	assert.NotEmpty(t, p.Cursor)

	store.add(6, 2)
	m.Notify(6)
	p = recv(t, sink.ch)
	assert.Len(t, p.Events, 2)
	assert.EqualValues(t, 6, p.Events[0].Ledger, "only ledger-6 events after the cursor")
	expectNoPush(t, sink.ch)
}

// A subscriber registered after commits exists sees nothing until the next
// commit: startLedger 0 anchors past the latest committed ledger.
func TestSubscribeAfterCommitsSeesOnlyNew(t *testing.T) {
	store := &fakeStore{}
	m := newTestManager(t, store)

	store.add(5, 3)
	m.Notify(5)

	sink := &chanSink{ch: make(chan Push, 16)}
	m.Subscribe(nil, 0, sink)
	expectNoPush(t, sink.ch) // ledger 5 predates the subscription

	store.add(6, 1)
	m.Notify(6)
	p := recv(t, sink.ch)
	require.Len(t, p.Events, 1)
	assert.EqualValues(t, 6, p.Events[0].Ledger)
}

// An explicit startLedger replays the backlog from there, across multiple
// pages, before going quiet until the next commit.
func TestHistoricalStartLedgerReplays(t *testing.T) {
	store := &fakeStore{}
	m := NewManager(silentLogger())
	m.Bind(store.source(), 2) // page size 2 forces pagination
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go m.Run(ctx)

	store.add(5, 3)
	store.add(6, 2)
	m.Notify(6)

	sink := &chanSink{ch: make(chan Push, 16)}
	m.Subscribe(nil, 5, sink)

	var got []protocol.EventInfo
	for len(got) < 5 {
		got = append(got, recv(t, sink.ch).Events...)
	}
	assert.Len(t, got, 5)
	assert.EqualValues(t, 5, got[0].Ledger)
	assert.EqualValues(t, 6, got[4].Ledger)
	expectNoPush(t, sink.ch)
}

// A sink that cannot keep up ends its own subscription, and only its own.
func TestSlowConsumerIsDropped(t *testing.T) {
	store := &fakeStore{}
	m := newTestManager(t, store)

	slow := &chanSink{ch: make(chan Push, 16), reject: true}
	healthy := &chanSink{ch: make(chan Push, 16)}
	m.Subscribe(nil, 0, slow)
	m.Subscribe(nil, 0, healthy)

	store.add(5, 1)
	m.Notify(5)

	p := recv(t, healthy.ch)
	assert.Len(t, p.Events, 1)
	require.Eventually(t, func() bool { return m.SubscriberCount() == 1 },
		5*time.Second, 10*time.Millisecond, "the slow subscription should be removed")
}

// A terminal source error is pushed as Err and ends the subscription.
func TestSourceErrorEndsSubscription(t *testing.T) {
	m := NewManager(silentLogger())
	m.Bind(func(context.Context, protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
		return protocol.GetEventsResponse{}, fmt.Errorf("startLedger must be within the ledger range: 7 - 9")
	}, 0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go m.Run(ctx)

	sink := &chanSink{ch: make(chan Push, 16)}
	m.Subscribe(nil, 2, sink)
	m.Notify(9)

	p := recv(t, sink.ch)
	assert.Contains(t, p.Err, "ledger range")
	require.Eventually(t, func() bool { return m.SubscriberCount() == 0 },
		5*time.Second, 10*time.Millisecond)
}
