package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
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
