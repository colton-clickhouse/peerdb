package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// CockroachDB permits TIME '24:00:00', and so does Postgres time, but PeerDB clamped it to 23:59:59.999999.
// Row 1 comes from the snapshot, row 2 from a CDC insert and row 3 from a CDC update.
func (s CRDBPGSuite) Test_Time_2400_Snapshot_And_CDC() {
	t := s.t
	srcTable := "time_2400"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, t TIME)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, '24:00:00'), (3, '12:00:00')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, '24:00:00')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET t = '24:00:00' WHERE id = 3`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 OR (id = 3 AND t <> '12:00:00')`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "24:00:00|24:00:00|24:00:00",
		s.destText(fmt.Sprintf(`SELECT string_agg(t::text, '|' ORDER BY id) FROM %s`, dstFull)))
}

// Mirrors from before InternalVersion_PgTime2400 keep clamping.
func (s CRDBPGSuite) Test_Time_2400_Old_Version() {
	t := s.t
	srcTable := "time_2400_old"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, t TIME)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, '24:00:00')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": fmt.Sprint(shared.InternalVersion_PgTime2400 - 1)}
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, '24:00:00')`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "23:59:59.999999|23:59:59.999999",
		s.destText(fmt.Sprintf(`SELECT string_agg(t::text, '|' ORDER BY id) FROM %s`, dstFull)))
}

// A TIME primary key holding both 23:59:59.999999 and 24:00:00: clamping would give both rows one key on the target,
// so CDC changes to either row would land on the same target row. Both come from the snapshot and are then updated.
func (s CRDBPGSuite) Test_Time_2400_Primary_Key() {
	t := s.t
	srcTable := "time_2400_pk"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (t TIME PRIMARY KEY, v INT8)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES ('23:59:59.999999', 1), ('24:00:00', 2)`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = v + 10 WHERE true`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc updates", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE v > 10`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "23:59:59.999999=11|24:00:00=12",
		s.destText(fmt.Sprintf(`SELECT string_agg(t::text || '=' || v, '|' ORDER BY t) FROM %s`, dstFull)))
}
