package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// TIMETZ into a target pre-created with time and text columns. The snapshot's COPY sent PG's 12-byte binary timetz
// whatever the target column was: a time column has no encoding for it, so the snapshot retried forever, and a text
// column would have taken the raw bytes. Both now get the UTC time of day as CDC writes it (in text, unpadded:
// 4:30:00.000000). Row 1 comes from the snapshot and row 2 from CDC; '23:30-05' crosses midnight in UTC.
func (s CRDBPGSuite) Test_TimeTZ_Into_Precreated_Time_And_Text() {
	t := s.t
	srcTable := "timetz_precreated"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const values = `'10:00:00+02', '23:30:00-05'`

	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, as_time TIMETZ, as_text TIMETZ)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, %s)`, srcFull, values)))
	// as PeerDB would create it, with the mirror's synced-at column
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, as_time time,
		as_text text, "_PEERDB_SYNCED_AT" timestamp DEFAULT CURRENT_TIMESTAMP)`, dstFull))
	require.NoError(t, err)

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, %s)`, srcFull, values)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	for id, via := range map[int]string{1: "snapshot", 2: "cdc"} {
		require.Equal(t, "08:00:00|4:30:00.000000", s.destText(fmt.Sprintf(
			`SELECT concat_ws('|', as_time::text, as_text) FROM %s WHERE id = %d`, dstFull, id)), via)
	}
}
