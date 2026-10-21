package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// INET[] maps to text[] on the target. The snapshot wrote host addresses with their full mask ({::1/128}), CDC
// without it ({::1}, as changefeed JSON and Postgres print them; CockroachDB's inet::text always adds the mask), so
// equality on the column depended on how the row arrived. Row 1 comes from the snapshot, row 2 from CDC; both must
// hold the same text. IPv4-mapped and IPv4-compatible IPv6 addresses and a zero-length mask are included because
// Go's netip (the snapshot) and CockroachDB (CDC) format them separately.
func (s CRDBPGSuite) Test_Inet_Array_Snapshot_Matches_CDC() {
	t := s.t
	srcTable := "inet_array"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	value := `ARRAY['10.0.0.1', '::1/128', '192.168.0.0/16', NULL, '2001:db8::/32', '10.1.2.3/8',
		'::ffff:1.2.3.4', '::1.2.3.4', '0.0.0.0/0', '::/0']::INET[]`

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, a INET[], v INET)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, %s, '10.0.0.1')`, srcFull, value)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, %s, '10.0.0.1')`, srcFull, value)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	const want = `{10.0.0.1,::1,192.168.0.0/16,NULL,2001:db8::/32,10.1.2.3/8,::ffff:1.2.3.4,::102:304,0.0.0.0/0,::/0}`
	for id, path := range map[int]string{1: "snapshot", 2: "cdc"} {
		require.Equal(t, want, s.destText(fmt.Sprintf(`SELECT a::text FROM %s WHERE id = %d`, dstFull, id)), path)
	}
}
