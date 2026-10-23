package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// #13: BYTES[] maps to text[]. CDC writes the elements in Postgres's bytea hex form (\x00ff), as CockroachDB's
// changefeed sends them, but the snapshot printed pgx's []byte with Go formatting ([0 255]). Both paths should
// write the hex form, which also casts to bytea[]. Row 1 comes from the snapshot, 2 from CDC.
func (s CRDBPGSuite) Test_Bytes_Array_Snapshot_Matches_CDC() {
	t := s.t
	srcTable := "bytes_array"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const value = `ARRAY['\x00ff'::BYTES, ''::BYTES, NULL, 'A'::BYTES]`

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, a BYTES[])`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, %s)`, srcFull, value)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, %s)`, srcFull, value)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	const want = `{"\\x00ff","\\x",NULL,"\\x41"}`
	for id, path := range map[int]string{1: "snapshot", 2: "cdc"} {
		require.Equal(t, want, s.destText(fmt.Sprintf(`SELECT a::text FROM %s WHERE id = %d`, dstFull, id)), path)
		require.Equal(t, "true", s.destText(fmt.Sprintf(`SELECT (a::bytea[] IS NOT DISTINCT FROM
			ARRAY['\x00ff'::bytea, ''::bytea, NULL, 'A'::bytea])::text FROM %s WHERE id = %d`, dstFull, id)), path+" as bytea[]")
	}
}
