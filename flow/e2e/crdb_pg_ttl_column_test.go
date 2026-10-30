package e2e

import (
	"fmt"
	"strconv"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Row-level TTL (WITH (ttl_expire_after = ...)) adds a hidden column, crdb_internal_expiration, that CockroachDB
// maintains itself (DEFAULT and ON UPDATE now() + the TTL). It is CockroachDB's own bookkeeping, but it was
// replicated as a regular NOT NULL column without a default, so the target carried a column the application
// doesn't know about, and inserts that don't set it fail after cutover. It should be left out on both paths,
// while the hidden rowid of a keyless table, which keys the target, stays. Row 1 comes from the snapshot, 2 from a
// CDC insert, then 1 is updated.
func (s CRDBPGSuite) ttlColumnMirror(srcTable string, keyless bool, version uint32) (columns string, rows string) {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	key := "id INT8 PRIMARY KEY"
	if keyless {
		key = "id INT8 NOT NULL"
	}
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (%s, v STRING) WITH (ttl_expire_after = '1 day')`, srcFull, key)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (1, 'snapshot')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (2, 'cdc')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'updated' WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id = 1 AND v = 'updated') OR (id = 2 AND v = 'cdc')`,
			dstFull)) == 2 && s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	columns = s.destText(fmt.Sprintf(`SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
		WHERE attrelid = '%s'::regclass AND attnum > 0 AND NOT attisdropped AND attname NOT ILIKE '\_peerdb\_%%'`, dstFull))
	rows = s.destText(fmt.Sprintf(`SELECT string_agg(id || '=' || v, ',' ORDER BY id) FROM %s`, dstFull))
	return columns, rows
}

func (s CRDBPGSuite) Test_TTL_Column_Left_Out() {
	columns, rows := s.ttlColumnMirror("ttl_column", false, shared.InternalVersion_Latest)
	require.Equal(s.t, "id,v", columns)
	require.Equal(s.t, "1=updated,2=cdc", rows)
}

func (s CRDBPGSuite) Test_TTL_Column_Left_Out_Keyless() {
	columns, rows := s.ttlColumnMirror("ttl_column_keyless", true, shared.InternalVersion_Latest)
	require.Equal(s.t, "id,v,rowid", columns, "the hidden rowid stays: it keys the target")
	require.Equal(s.t, "1=updated,2=cdc", rows)
}

// Mirrors created before InternalVersion_CockroachOmitInternalColumns keep replicating the column.
func (s CRDBPGSuite) Test_TTL_Column_Old_Version() {
	columns, rows := s.ttlColumnMirror("ttl_column_old", false, shared.InternalVersion_CockroachOmitInternalColumns-1)
	require.Equal(s.t, "id,v,crdb_internal_expiration", columns)
	require.Equal(s.t, "1=updated,2=cdc", rows)
}

// Row-level TTL enabled on a table the mirror already replicates: the changefeed's rows gain the hidden column
// mid-stream, and it stays out of the target as when the mirror started with it.
func (s CRDBPGSuite) Test_TTL_Column_Added_Mid_Stream() {
	t := s.t
	srcTable := "ttl_column_added"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v STRING)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'snapshot')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s SET (ttl_expire_after = '1 day')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'cdc')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'updated' WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id = 1 AND v = 'updated') OR (id = 2 AND v = 'cdc')`,
			dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Equal(t, "id,v", s.destText(fmt.Sprintf(`SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
		WHERE attrelid = '%s'::regclass AND attnum > 0 AND NOT attisdropped AND attname NOT ILIKE '\_peerdb\_%%'`, dstFull)))
}
