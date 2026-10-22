package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// BC dates and timestamps replicate in the snapshot, but CDC wrote them into the raw table with Go's
// astronomical years ("-0043-03-15" for 44 BC), which Postgres rejects in normalize, stalling the mirror; a BC
// TIMESTAMP[] element failed the sync itself (time.Time can't marshal years before 0 to JSON). Rows 1 and 2 come
// from the snapshot, 3 from a CDC insert and 2 is then updated through CDC.
func (s CRDBPGSuite) Test_BC_Dates_Snapshot_And_CDC() {
	t := s.t
	srcTable := "bc_values"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const bc = `'0044-03-15 BC', '0044-03-15 12:00:00.5 BC', '0001-01-01 00:00:00+00 BC',
		ARRAY['0044-03-15 BC', '2026-01-01', NULL]::DATE[], ARRAY['4713-01-01 00:00:00 BC', '2026-01-01 00:00:00']::TIMESTAMP[],
		ARRAY['0001-12-31 23:59:59.999999+00 BC', NULL]::TIMESTAMPTZ[]`

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, d DATE, ts TIMESTAMP,
		tz TIMESTAMPTZ, da DATE[], tsa TIMESTAMP[], tza TIMESTAMPTZ[])`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, %s), (2, '2026-01-01', '2026-01-01', '2026-01-01', '{}', '{}', '{}')`, srcFull, bc)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (3, %s)`, srcFull, bc)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET (d, ts, tz, da, tsa, tza) = (%s) WHERE id = 2`, srcFull, bc)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 3 OR (id = 2 AND d < '0001-01-01')`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	const want = `0044-03-15 BC|0044-03-15 12:00:00.5 BC|0001-01-01 00:00:00 BC|{"0044-03-15 BC",2026-01-01,NULL}|` +
		`{"4713-01-01 00:00:00 BC","2026-01-01 00:00:00"}|{"0001-12-31 23:59:59.999999 BC",NULL}`
	for id := 1; id <= 3; id++ {
		require.Equal(t, want, s.destText(fmt.Sprintf(`SELECT d::text || '|' || ts::text || '|' || (tz AT TIME ZONE 'UTC')::text
			|| '|' || da::text || '|' || tsa::text || '|'
			|| (SELECT array_agg(e AT TIME ZONE 'UTC' ORDER BY o) FROM unnest(tza) WITH ORDINALITY u(e, o))::text
			FROM %s WHERE id = %d`, dstFull, id)), "id %d", id)
	}
}
