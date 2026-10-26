package conncockroachdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// getTableSchema with an asOf timestamp is what the changefeed uses to type columns added since the schema was
// cached. AS OF SYSTEM TIME has to follow the whole FROM clause, joins included, or every such read fails and CDC
// stops at the first row of a table that gained a column. Needs a CockroachDB (CI_COCKROACH_HOST), so it only runs
// with the e2e services.
func TestGetTableSchemaAsOf(t *testing.T) {
	if os.Getenv("CI_COCKROACH_HOST") == "" {
		t.Skip("needs CockroachDB: set CI_COCKROACH_HOST (e2e services)")
	}
	ctx := t.Context()
	c, err := NewCockroachDBConnector(ctx, nil, internal.GetCockroachDBConfigFromEnv())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	schema := fmt.Sprintf("schema_asof_%d", time.Now().UnixNano())
	_, err = c.conn.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	// t.Context() is already canceled when cleanups run
	t.Cleanup(func() { _, _ = c.conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })

	tables := map[string]struct {
		ddl    string
		pkCols []string
	}{
		"plain_pk":     {"(id INT8 PRIMARY KEY, v STRING)", []string{"id"}},
		"composite_pk": {"(a INT8, b STRING, v STRING, PRIMARY KEY (a, b))", []string{"a", "b"}},
		// the shard column is a virtual key column: the destination is keyed without it
		"hash_sharded_pk": {"(ts TIMESTAMPTZ, id INT8, v STRING, PRIMARY KEY (ts, id) USING HASH)", []string{"ts", "id"}},
	}
	for name, table := range tables {
		_, err := c.conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s %s", schema, name, table.ddl))
		require.NoError(t, err)
	}
	before, err := c.clusterLogicalTimestamp(ctx)
	require.NoError(t, err)
	for name := range tables {
		_, err := c.conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN added INT8", schema, name))
		require.NoError(t, err)
	}
	after, err := c.clusterLogicalTimestamp(ctx)
	require.NoError(t, err)

	for name, table := range tables {
		for _, tc := range []struct {
			at    crdbHLC
			added bool
		}{{before, false}, {after, true}, {crdbHLC{}, true}} {
			source := schema + "." + name
			schemas, err := c.getTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
				[]*protos.TableMapping{{SourceTableIdentifier: source}}, tc.at)
			require.NoError(t, err, "%s as of %q", name, tc.at)
			names := make([]string, 0, len(schemas[source].Columns))
			for _, col := range schemas[source].Columns {
				names = append(names, col.Name)
			}
			require.Equal(t, tc.added, strings.HasSuffix(strings.Join(names, ","), ",added"),
				"%s as of %q: columns %v", name, tc.at, names)
			require.Equal(t, table.pkCols, schemas[source].PrimaryKeyColumns, "%s as of %q", name, tc.at)
		}
	}
}
