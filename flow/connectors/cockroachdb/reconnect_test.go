package conncockroachdb

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func crdbForReconnectTests(t *testing.T) *CockroachDBConnector {
	t.Helper()
	if os.Getenv("CI_COCKROACH_HOST") == "" {
		t.Skip("needs CockroachDB: set CI_COCKROACH_HOST (e2e services)")
	}
	c, err := NewCockroachDBConnector(t.Context(), nil, internal.GetCockroachDBConfigFromEnv())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// After the connector's own connection dies, the connection check before a sync and the schema read reconnect
// instead of failing every sync of the activity with "connection to source down".
func TestConnectorReconnectsDeadConnection(t *testing.T) {
	table := "public.reconnect_probe"
	schemaRead := func(t *testing.T, c *CockroachDBConnector) {
		t.Helper()
		schemas, err := c.getTableSchema(t.Context(), nil, 0, protos.TypeSystem_Q,
			[]*protos.TableMapping{{SourceTableIdentifier: table}}, crdbHLC{})
		require.NoError(t, err)
		require.Len(t, schemas[table].Columns, 2)
	}
	setup := crdbForReconnectTests(t)
	_, err := setup.conn.Exec(t.Context(), "CREATE TABLE IF NOT EXISTS "+table+" (id INT8 PRIMARY KEY, v STRING)")
	require.NoError(t, err)

	t.Run("closed", func(t *testing.T) {
		c := crdbForReconnectTests(t)
		old := c.conn
		require.NoError(t, old.Close(t.Context()))
		require.NoError(t, c.ConnectionActive(t.Context()))
		require.NotSame(t, old, c.conn, "a new connection")
		schemaRead(t, c)
	})

	// the server or a load balancer closes it, as when its node fails or the mirror idles past the balancer's
	// timeout: the client only finds out on its next write
	t.Run("closed by the server", func(t *testing.T) {
		c := crdbForReconnectTests(t)
		var session string
		require.NoError(t, c.conn.QueryRow(t.Context(), "SHOW session_id").Scan(&session))
		killer := crdbForReconnectTests(t)
		_, err := killer.conn.Exec(t.Context(), "CANCEL SESSION $1", session)
		require.NoError(t, err)
		old := c.conn
		require.Eventually(t, func() bool {
			var n int
			require.NoError(t, killer.conn.QueryRow(t.Context(),
				"SELECT count(*) FROM [SHOW CLUSTER SESSIONS] WHERE session_id = $1", session).Scan(&n))
			return n == 0
		}, 10*time.Second, 100*time.Millisecond, "the server ended the session")
		require.False(t, old.conn.IsClosed(), "the client hasn't noticed yet")

		schemaRead(t, c)
		require.NotSame(t, old, c.conn, "a new connection")
		require.NoError(t, c.ConnectionActive(t.Context()))
	})
}
