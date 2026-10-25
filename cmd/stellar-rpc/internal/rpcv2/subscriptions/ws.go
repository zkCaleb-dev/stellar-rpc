package subscriptions

// The WebSocket transport (PROTOTYPE): one connection carries any number of
// subscriptions, speaking JSON-RPC 2.0-shaped frames:
//
//	→ {"jsonrpc":"2.0","id":1,"method":"subscribeEvents",
//	   "params":{"filters":[...],"startLedger":0}}
//	← {"jsonrpc":"2.0","id":1,"result":{"subscriptionId":"sub-1"}}
//	← {"jsonrpc":"2.0","method":"eventsNotification",
//	   "params":{"subscriptionId":"sub-1","latestLedger":N,
//	             "cursor":"...","events":[...]}}
//	→ {"jsonrpc":"2.0","id":2,"method":"unsubscribeEvents",
//	   "params":{"subscriptionId":"sub-1"}}
//	← {"jsonrpc":"2.0","id":2,"result":true}
//
// filters and events are getEvents' own wire shapes, and every notification
// carries the getEvents cursor it was served from, so a dropped connection
// resumes over plain getEvents. A subscription that fails terminally (e.g.
// startLedger below retention) is reported as an eventsNotification with
// "error" set and then removed; the connection stays up. A connection whose
// notifications back up beyond its buffer is closed (slow-consumer policy).

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// connOutBuffer is the per-connection notification buffer; a connection that
// falls this far behind is closed rather than back-pressuring the fan-out.
const connOutBuffer = 128

// wsWriteTimeout bounds each notification write.
const wsWriteTimeout = 10 * time.Second

type wsRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type wsResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *wsError        `json:"error,omitempty"`
}

type wsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type wsNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  Push   `json:"params"`
}

type subscribeParams struct {
	Filters     []protocol.EventFilter `json:"filters"`
	StartLedger uint32                 `json:"startLedger,omitempty"`
}

type unsubscribeParams struct {
	SubscriptionID string `json:"subscriptionId"`
}

// wsConn adapts one WebSocket connection into a Sink: Deliver is a
// non-blocking send into the connection's buffered out channel, drained by
// the connection's single writer goroutine. On overflow it cancels the
// connection (closing every subscription it owns) and reports false.
type wsConn struct {
	out    chan Push
	cancel context.CancelFunc
}

func (c *wsConn) Deliver(p Push) bool {
	select {
	case c.out <- p:
		return true
	default:
		c.cancel()
		return false
	}
}

// HTTPHandler upgrades requests into event-subscription connections.
//
// PROTOTYPE: OriginPatterns is wide open; a production endpoint decides its
// origin policy (and its place behind the operator's proxy) explicitly.
func (m *Manager) HTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: []string{"*"},
		})
		if err != nil {
			return // Accept already answered the request
		}
		m.serveConn(r.Context(), conn)
	})
}

func (m *Manager) serveConn(parent context.Context, conn *websocket.Conn) {
	ctx, cancel := context.WithCancel(parent)
	sink := &wsConn{out: make(chan Push, connOutBuffer), cancel: cancel}

	var mu sync.Mutex // guards owned (reader registers, defer sweeps)
	owned := map[string]struct{}{}
	defer func() {
		cancel()
		mu.Lock()
		for id := range owned {
			m.Unsubscribe(id)
		}
		mu.Unlock()
		_ = conn.CloseNow()
	}()

	// The connection's single writer: notifications only. Replies to
	// subscribe/unsubscribe are written by the reader loop below, which is
	// safe because the writer is the only other writer and wsjson writes are
	// whole frames — serialize the two with writeMu.
	var writeMu sync.Mutex
	write := func(v any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		wctx, wcancel := context.WithTimeout(ctx, wsWriteTimeout)
		defer wcancel()
		return wsjson.Write(wctx, conn, v)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-sink.out:
				if err := write(wsNotification{JSONRPC: "2.0", Method: "eventsNotification", Params: p}); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	for {
		var req wsRequest
		if err := wsjson.Read(ctx, conn, &req); err != nil {
			return // client went away, or ctx canceled (slow consumer, shutdown)
		}
		switch req.Method {
		case "subscribeEvents":
			var p subscribeParams
			if err := json.Unmarshal(req.Params, &p); err != nil {
				_ = write(wsResponse{JSONRPC: "2.0", ID: req.ID, Error: &wsError{Code: -32602, Message: err.Error()}})
				continue
			}
			id := m.Subscribe(p.Filters, p.StartLedger, sink)
			mu.Lock()
			owned[id] = struct{}{}
			mu.Unlock()
			_ = write(wsResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]string{"subscriptionId": id}})
		case "unsubscribeEvents":
			var p unsubscribeParams
			if err := json.Unmarshal(req.Params, &p); err != nil {
				_ = write(wsResponse{JSONRPC: "2.0", ID: req.ID, Error: &wsError{Code: -32602, Message: err.Error()}})
				continue
			}
			m.Unsubscribe(p.SubscriptionID)
			mu.Lock()
			delete(owned, p.SubscriptionID)
			mu.Unlock()
			_ = write(wsResponse{JSONRPC: "2.0", ID: req.ID, Result: true})
		default:
			_ = write(wsResponse{JSONRPC: "2.0", ID: req.ID, Error: &wsError{Code: -32601, Message: "method not found"}})
		}
	}
}
