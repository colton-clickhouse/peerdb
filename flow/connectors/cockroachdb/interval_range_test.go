package conncockroachdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Intervals through the snapshot (its column list and row decoding) and the changefeed (its JSON) give the same
// value, or both fail. pgwire's binary interval holds days and months in 32 bits, so a larger value arrived in the
// snapshot wrapped, silently: 3000000000 days as -1294967296 days.
func TestIntervalSnapshotMatchesChangefeed(t *testing.T) {
	if os.Getenv("CI_COCKROACH_HOST") == "" {
		t.Skip("needs CockroachDB: set CI_COCKROACH_HOST (e2e services)")
	}
	c, err := NewCockroachDBConnector(t.Context(), nil, internal.GetCockroachDBConfigFromEnv())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	table := fmt.Sprintf("public.interval_probe_%d", time.Now().UnixNano())
	_, err = c.conn.Exec(t.Context(), "CREATE TABLE "+table+" (id INT8 PRIMARY KEY, i INTERVAL, ia INTERVAL[])")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = c.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })
	schemas, err := c.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: table}})
	require.NoError(t, err)
	tableSchema := schemas[table]
	pg := newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES)

	for id, expr := range []string{
		"'3000000000 days'", "'2147483647 days'", "'-2147483649 days'", "'178000000 years'", "'3000000000 hours'",
		"'1 day 02:03:04'", "'1 year 2 mons -3 days -04:05:06.789'", "'-1 days +02:00:00'", "'00:00:00.000001'",
		"'0'", "'-178000000 years'",
	} {
		t.Run(expr, func(t *testing.T) {
			if _, err := c.conn.Exec(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES (%d, %s, ARRAY[%[3]s, NULL]::INTERVAL[])",
				table, id, expr)); err != nil {
				t.Skipf("CockroachDB rejects it: %v", err)
			}
			rows, err := c.conn.Query(t.Context(), fmt.Sprintf("SELECT %s FROM %s WHERE id = %d",
				buildSelectedColumns(tableSchema.Columns, nil), table, id))
			require.NoError(t, err)
			fds := rows.FieldDescriptions()
			schema := qRecordSchemaFromFieldDescriptions(tableSchema, fds)
			require.True(t, rows.Next())
			snap, snapErr := c.mapRowToQRecord(rows, fds, schema, protos.DBType_POSTGRES, true, pg)
			rows.Close()

			var scalar, array string
			require.NoError(t, c.conn.QueryRow(t.Context(), fmt.Sprintf(
				"SELECT to_json(i)::STRING, to_json(ia)::STRING FROM %s WHERE id = %d", table, id)).Scan(&scalar, &array))
			cdc, cdcErr := qvalueFromChangefeedJSON(types.QField{Name: "i", Type: types.QValueKindInterval, Nullable: true},
				json.RawMessage(scalar), pg)
			cdcArr, cdcArrErr := qvalueFromChangefeedJSON(
				types.QField{Name: "ia", Type: types.QValueKindArrayInterval, Nullable: true}, json.RawMessage(array), pg)
			if snapErr != nil || cdcErr != nil || cdcArrErr != nil {
				require.True(t, snapErr != nil && (cdcErr != nil || cdcArrErr != nil),
					"one path fails, the other doesn't: snapshot %v, changefeed %v / %v", snapErr, cdcErr, cdcArrErr)
				return
			}
			require.Equal(t, cdc, snap[1], "scalar")
			require.Equal(t, cdcArr.Value(), snap[2].Value(), "array")
		})
	}
}

// The snapshot reads INTERVAL columns as text, which follows the session's intervalstyle: a role whose default is
// iso_8601 or sql_standard gave P1DT2H or 1 2:00:00, which the snapshot's parser doesn't read. The connector pins the
// postgres style for its sessions.
func TestConnectorPinsIntervalStyle(t *testing.T) {
	if os.Getenv("CI_COCKROACH_HOST") == "" {
		t.Skip("needs CockroachDB: set CI_COCKROACH_HOST (e2e services)")
	}
	admin, err := NewCockroachDBConnector(t.Context(), nil, internal.GetCockroachDBConfigFromEnv())
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	user := fmt.Sprintf("intervalstyle_%d", time.Now().UnixNano())
	for _, stmt := range []string{"CREATE USER " + user, "ALTER ROLE " + user + " SET intervalstyle = 'iso_8601'"} {
		_, err := admin.conn.Exec(t.Context(), stmt)
		require.NoError(t, err)
	}
	t.Cleanup(func() { _, _ = admin.conn.Exec(context.Background(), "DROP USER IF EXISTS "+user) })

	config := internal.GetCockroachDBConfigFromEnv()
	config.User, config.Password = user, ""
	c, err := NewCockroachDBConnector(t.Context(), nil, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	var text string
	require.NoError(t, c.conn.QueryRow(t.Context(), "SELECT '1 day 02:00:00'::INTERVAL::STRING").Scan(&text))
	require.Equal(t, "1 day 02:00:00", text)
}
