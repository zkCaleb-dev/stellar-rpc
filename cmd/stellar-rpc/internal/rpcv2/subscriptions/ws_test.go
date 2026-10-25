package subscriptions

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wsFrame is whatever the server writes: a reply (ID set) or a notification
// (Method set).
type wsFrame struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *wsError        `json:"error,omitempty"`
	Params *Push           `json:"params,omitempty"`
}

// readFrame reads frames until pred matches, tolerating reply/notification
// interleave (the subscribe reply and a first notification race by design).
func readFrame(ctx context.Context, t *testing.T, conn *websocket.Conn, pred func(wsFrame) bool) wsFrame {
	t.Helper()
	for {
		var f wsFrame
		require.NoError(t, wsjson.Read(ctx, conn, &f))
		if pred(f) {
			return f
		}
	}
}

func TestWebSocketSubscribeNotifyUnsubscribe(t *testing.T) {
	store := &fakeStore{}
	m := newTestManager(t, store)

	srv := httptest.NewServer(m.HTTPHandler())
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })

	// Subscribe to everything from now on.
	require.NoError(t, wsjson.Write(ctx, conn, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "subscribeEvents",
		"params": map[string]any{"filters": []any{}},
	}))
	reply := readFrame(ctx, t, conn, func(f wsFrame) bool { return len(f.ID) > 0 })
	require.Nil(t, reply.Error)
	var res struct {
		SubscriptionID string `json:"subscriptionId"`
	}
	require.NoError(t, json.Unmarshal(reply.Result, &res))
	require.NotEmpty(t, res.SubscriptionID)

	// Commit a ledger; the connection must receive its events pushed.
	store.add(42, 2)
	m.Notify(42)
	notif := readFrame(ctx, t, conn, func(f wsFrame) bool { return f.Method == "eventsNotification" })
	require.NotNil(t, notif.Params)
	assert.Equal(t, res.SubscriptionID, notif.Params.SubscriptionID)
	assert.Len(t, notif.Params.Events, 2)
	assert.NotEmpty(t, notif.Params.Cursor, "every push carries a getEvents-resumable cursor")

	// Unsubscribe; later commits must not reach the connection.
	require.NoError(t, wsjson.Write(ctx, conn, map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "unsubscribeEvents",
		"params": map[string]any{"subscriptionId": res.SubscriptionID},
	}))
	readFrame(ctx, t, conn, func(f wsFrame) bool { return len(f.ID) > 0 })
	require.Eventually(t, func() bool { return m.SubscriberCount() == 0 },
		5*time.Second, 10*time.Millisecond)

	store.add(43, 1)
	m.Notify(43)
	quiet, qcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer qcancel()
	var f wsFrame
	err = wsjson.Read(quiet, conn, &f)
	require.Error(t, err, "no frames after unsubscribe")
}

func TestWebSocketConnectionCloseSweepsItsSubscriptions(t *testing.T) {
	store := &fakeStore{}
	m := newTestManager(t, store)

	srv := httptest.NewServer(m.HTTPHandler())
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, url, nil)
	require.NoError(t, err)

	require.NoError(t, wsjson.Write(ctx, conn, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "subscribeEvents", "params": map[string]any{},
	}))
	readFrame(ctx, t, conn, func(f wsFrame) bool { return len(f.ID) > 0 })
	require.Equal(t, 1, m.SubscriberCount())

	require.NoError(t, conn.Close(websocket.StatusNormalClosure, "bye"))
	require.Eventually(t, func() bool { return m.SubscriberCount() == 0 },
		5*time.Second, 10*time.Millisecond, "a closed connection sweeps its subscriptions")
}
