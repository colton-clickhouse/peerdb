package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func crdbForReconnectTests(t *testing.T, config *protos.CockroachDBConfig) *CockroachDBConnector {
	t.Helper()
	if os.Getenv("CI_COCKROACH_HOST") == "" {
		t.Skip("needs CockroachDB: set CI_COCKROACH_HOST (e2e services)")
	}
	if config == nil {
		config = internal.GetCockroachDBConfigFromEnv()
	}
	c, err := NewCockroachDBConnector(t.Context(), nil, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// blackholeProxy forwards TCP to the test cluster until blackhole() is called: from then on the connections it holds
// stay open but nothing passes, as when a node dies without resetting them. Connections accepted later forward.
type blackholeProxy struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	dead     atomic.Bool
}

func newBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &blackholeProxy{listener: listener}
	t.Cleanup(func() {
		_ = listener.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, server)
			p.mu.Unlock()
			held := p.dead.Load() // accepted after blackhole(): forwards
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := src.Read(buf)
					if err != nil {
						return
					}
					if !held && p.dead.Load() {
						// swallow everything, keep the socket open
						_, _ = io.Copy(io.Discard, src)
						return
					}
					if _, err := dst.Write(buf[:n]); err != nil {
						return
					}
				}
			}
			go pipe(server, client)
			go pipe(client, server)
		}
	}()
	return p
}

func (p *blackholeProxy) blackhole() { p.dead.Store(true) }

func (p *blackholeProxy) port() uint32 { return uint32(p.listener.Addr().(*net.TCPAddr).Port) }

// After the connector's own connection dies, the connection check before a sync and the schema read reconnect
// instead of failing every sync of the activity with "connection to source down".
func TestConnectorReconnectsDeadConnection(t *testing.T) {
	table := fmt.Sprintf("public.reconnect_probe_%d", time.Now().UnixNano())
	schemaRead := func(t *testing.T, c *CockroachDBConnector) {
		t.Helper()
		schemas, err := c.getTableSchema(t.Context(), nil, 0, protos.TypeSystem_Q,
			[]*protos.TableMapping{{SourceTableIdentifier: table}}, crdbHLC{})
		require.NoError(t, err)
		require.Len(t, schemas[table].Columns, 2)
	}
	setup := crdbForReconnectTests(t, nil)
	_, err := setup.conn.Exec(t.Context(), "CREATE TABLE "+table+" (id INT8 PRIMARY KEY, v STRING)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = setup.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

	t.Run("closed", func(t *testing.T) {
		c := crdbForReconnectTests(t, nil)
		old := c.conn
		require.NoError(t, old.Close(t.Context()))
		require.NoError(t, c.ConnectionActive(t.Context()))
		require.NotSame(t, old, c.conn, "a new connection")
		schemaRead(t, c)
	})

	// the server or a load balancer closes it, as when its node fails or the mirror idles past the balancer's
	// timeout: the client only finds out on its next write
	t.Run("closed by the server", func(t *testing.T) {
		c := crdbForReconnectTests(t, nil)
		var session string
		require.NoError(t, c.conn.QueryRow(t.Context(), "SHOW session_id").Scan(&session))
		killer := crdbForReconnectTests(t, nil)
		_, err := killer.conn.Exec(t.Context(), "CANCEL SESSION $1", session)
		require.NoError(t, err)
		old := c.conn
		require.Eventually(t, func() bool {
			var n int
			err := killer.conn.QueryRow(t.Context(),
				"SELECT count(*) FROM [SHOW CLUSTER SESSIONS] WHERE session_id = $1", session).Scan(&n)
			return assert.NoError(t, err) && n == 0
		}, 10*time.Second, 100*time.Millisecond, "the server ended the session")
		require.False(t, old.conn.IsClosed(), "the client hasn't noticed yet")

		schemaRead(t, c)
		require.NotSame(t, old, c.conn, "a new connection")
		require.NoError(t, c.ConnectionActive(t.Context()))
	})

	// nothing answers and nothing resets the connection: the ping gives up after connPingTimeout instead of TCP's
	// many minutes, and the reconnect goes through
	t.Run("blackholed", func(t *testing.T) {
		config := internal.GetCockroachDBConfigFromEnv()
		proxy := newBlackholeProxy(t, net.JoinHostPort(config.Host, strconv.Itoa(int(config.Port))))
		config.Host, config.Port = "127.0.0.1", proxy.port()
		c := crdbForReconnectTests(t, config)
		old := c.conn
		proxy.blackhole()
		start := time.Now()
		require.NoError(t, c.ConnectionActive(t.Context()))
		require.Less(t, time.Since(start), connPingTimeout+10*time.Second)
		require.NotSame(t, old, c.conn, "a new connection")
		schemaRead(t, c)
	})

	t.Run("reconnect refused", func(t *testing.T) {
		c := crdbForReconnectTests(t, nil)
		c.dialConfig = c.dialConfig.Copy()
		c.dialConfig.User = "no_such_user_reconnect"
		require.NoError(t, c.conn.Close(t.Context()))
		err := c.ConnectionActive(t.Context())
		require.Error(t, err)
		require.True(t, errors.As(err, new(*exceptions.AuthError)), "classified like the first connect: %v", err)
	})

	t.Run("canceled", func(t *testing.T) {
		c := crdbForReconnectTests(t, nil)
		old := c.conn
		require.NoError(t, old.Close(t.Context()))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.Error(t, c.ConnectionActive(ctx))
		require.Same(t, old, c.conn, "no reconnect for a canceled caller")
	})
}
