package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// #7: INTERVAL and INTERVAL[] landed as text / text[] holding PeerDB's interval JSON ({"hours":4,...}). They should
// be interval / interval[] with the source's values. Rows 1 and 2 come from the snapshot, 3 from a CDC insert and
// 2 is then updated through CDC.
func (s CRDBPGSuite) Test_Interval_Snapshot_And_CDC() {
	t := s.t
	srcTable := "interval_values"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const (
		i1 = `'1 year 2 mons 3 days 04:05:06.789012'`
		a1 = `ARRAY['1 day', '-01:00:00', NULL, '1 mon -1 day 00:00:00.000001']::INTERVAL[]`
		i3 = `'-178000000 years'`
		a3 = `ARRAY['106751991 days 04:00:54.775807']::INTERVAL[]`
	)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, i INTERVAL, a INTERVAL[])`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, %s, %s), (2, '0', ARRAY[]::INTERVAL[])`, srcFull, i1, a1)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, %s, %s)`, srcFull, i3, a3)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET i = %s, a = %s WHERE id = 2`, srcFull, i1, a1)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 3 OR (id = 2 AND a::text <> '{}')`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "interval|interval[]", s.destText(fmt.Sprintf(
		`SELECT format_type(i.atttypid, i.atttypmod) || '|' || format_type(a.atttypid, a.atttypmod) FROM pg_attribute i, pg_attribute a
		WHERE i.attrelid = '%[1]s'::regclass AND i.attname = 'i' AND a.attrelid = '%[1]s'::regclass AND a.attname = 'a'`, dstFull)))
	want := map[int]string{
		1: `1 year 2 mons 3 days 04:05:06.789012|{"1 day",-01:00:00,NULL,"1 mon -1 days +00:00:00.000001"}`,
		2: `1 year 2 mons 3 days 04:05:06.789012|{"1 day",-01:00:00,NULL,"1 mon -1 days +00:00:00.000001"}`,
		3: `-178000000 years|{"106751991 days 04:00:54.775807"}`,
	}
	for id, w := range want {
		require.Equal(t, w, s.destText(fmt.Sprintf(`SELECT i::text || '|' || a::text FROM %s WHERE id = %d`, dstFull, id)), "id %d", id)
	}
}

// Mirrors from before InternalVersion_PgInterval keep text holding PeerDB's interval JSON.
func (s CRDBPGSuite) Test_Interval_Old_Version() {
	t := s.t
	srcTable := "interval_old"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, i INTERVAL)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, '3 days 04:00:00')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": fmt.Sprint(shared.InternalVersion_PgInterval - 1)}
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, '3 days 04:00:00')`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "text", s.destText(fmt.Sprintf(
		`SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '%s'::regclass AND attname = 'i'`, dstFull)))
	const old = `{"hours":4,"days":3,"valid":true}`
	require.Equal(t, old+"|"+old, s.destText(fmt.Sprintf(`SELECT string_agg(i, '|' ORDER BY id) FROM %s`, dstFull)))
}
