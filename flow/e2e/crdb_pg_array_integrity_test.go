package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Arrays whose CockroachDB snapshot values were silently wrong: OID[] elements all 0, TIME[] and VARBIT[] elements
// in Go's formatting (the changefeed was right). TIME[] also holds 24:00:00 next to 23:59:59.999999, which must stay
// apart. Row 1 comes from the snapshot and is checked before any CDC touches it (a CDC update re-sends the whole
// row, which would hide a snapshot error), 2 from a CDC insert, then 1 is updated; all of them must land as the
// source has them.
func (s CRDBPGSuite) Test_Array_Integrity_Snapshot_Matches_CDC() {
	t := s.t
	srcTable := "array_integrity"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const want = `{1,2147483648,4294967295}|{12:34:56,00:00:01.5,NULL,24:00:00,23:59:59.999999}|{101,1,NULL}` +
		`|{2024-01-02,infinity,-infinity,NULL}|{101,010}|{a,b,NULL}`
	const values = `ARRAY[1, 2147483648, 4294967295]::OID[],
		ARRAY['12:34:56', '00:00:01.5', NULL, '24:00:00', '23:59:59.999999']::TIME[],
		ARRAY[B'101', B'1', NULL]::VARBIT[], ARRAY['2024-01-02', 'infinity', '-infinity', NULL]::DATE[],
		ARRAY[B'101', B'010']::BIT(3)[], ARRAY['a', 'b', NULL]::"char"[]`

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v INT8,
		o OID[], tm TIME[], vb VARBIT[], d DATE[], bt BIT(3)[], ch "char"[])`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 0, %s)`, srcFull, values)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	row := func(id int) string {
		return s.destText(fmt.Sprintf(`SELECT concat_ws('|', o::text, tm::text, vb::text, d::text, bt::text, ch::text) FROM %s WHERE id = %d`,
			dstFull, id))
	}
	snapshotRow := row(1)
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 0, %s)`, srcFull, values)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 1 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 OR (id = 1 AND v = 1)`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, want, snapshotRow, "snapshot row")
	require.Equal(t, want, row(2), "CDC insert")
	require.Equal(t, want, row(1), "CDC update")
}
