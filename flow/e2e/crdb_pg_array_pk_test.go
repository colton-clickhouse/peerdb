package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Array primary keys through CDC. Normalize partitions the raw rows by the key, cast from the raw JSON text: an array
// arrives there as a JSON array (["10.0.0.1","::1"]), which isn't array text, so every batch with such a row failed
// normalize and the mirror stopped. The snapshot was fine. Rows 1 and 2 come from the snapshot; 1 is updated and 2
// deleted through CDC.
func (s CRDBPGSuite) Test_Inet_Array_Primary_Key() {
	t := s.t
	srcTable := "inet_array_pk"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (a INET[] PRIMARY KEY, n INT8)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (ARRAY['10.0.0.1', '::1']::INET[], 0), (ARRAY['10.0.0.2']::INET[], 0)`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET n = 1 WHERE n = 0 AND array_length(a, 1) = 2`,
		srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE array_length(a, 1) = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc update and delete", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE n = 1`, dstFull)) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE a::text = '{10.0.0.2}'`, dstFull)) == 0
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Equal(t, "{10.0.0.1,::1}=1", s.destText(fmt.Sprintf(`SELECT string_agg(a::text || '=' || n, ',') FROM %s`, dstFull)))
}

func (s CRDBPGSuite) Test_Bytes_Array_Primary_Key() {
	t := s.t
	srcTable := "bytes_array_pk"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (a BYTES[] PRIMARY KEY, n INT8)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (ARRAY['\x00ff', 'A']::BYTES[], 0), (ARRAY['\x01']::BYTES[], 0)`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET n = 1 WHERE array_length(a, 1) = 2`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`DELETE FROM %s WHERE array_length(a, 1) = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc update and delete", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE n = 1`, dstFull)) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Equal(t, `{"\\x00ff","\\x41"}=1`, s.destText(fmt.Sprintf(`SELECT string_agg(a::text || '=' || n, ',') FROM %s`,
		dstFull)))
}
