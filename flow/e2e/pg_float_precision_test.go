package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Postgres sources: floats must arrive exactly, in the snapshot and through CDC, whatever the source role's or
// database's extra_float_digits. pgoutput sends values in text form, which a setting of 0 or less rounds to 15
// (double precision) or 6 (real) significant digits.
func (s PeerFlowE2ETestSuitePG) Test_Float_Precision_Low_Extra_Float_Digits() {
	s.floatPrecision("float_prec_low", true)
}

func (s PeerFlowE2ETestSuitePG) Test_Float_Precision_Default() {
	s.floatPrecision("float_prec_default", false)
}

func (s PeerFlowE2ETestSuitePG) floatPrecision(name string, lowDigits bool) {
	tc := NewTemporalClient(s.t)
	src, dst := s.attachSchemaSuffix(name), s.attachSchemaSuffix(name+"_dst")
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id int PRIMARY KEY, d double precision, r real, da double precision[], ra real[])`, src))
	require.NoError(s.t, err)
	if lowDigits {
		var role string
		require.NoError(s.t, s.Conn().QueryRow(s.t.Context(), `SELECT current_user`).Scan(&role))
		_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`ALTER ROLE %s SET extra_float_digits = 0`, role))
		require.NoError(s.t, err)
		s.t.Cleanup(func() {
			_, _ = s.Conn().Exec(context.Background(), fmt.Sprintf(`ALTER ROLE %s RESET extra_float_digits`, role))
		})
	}
	const values = `(0.1::float8 + 0.2::float8, 1/3::real, ARRAY[pi(), 1e-300]::float8[], ARRAY[0.1, 1/3]::real[])`
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s SELECT 1, * FROM (VALUES %s) v`, src, values))
	require.NoError(s.t, err)

	flowConnConfig := (&FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix(name), TableNameMapping: map[string]string{src: dst}, Destination: s.Peer().Name,
	}).GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s SELECT 2, * FROM (VALUES %s) v`, src, values))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 2*time.Minute, "snapshot and CDC rows", func() bool {
		var n int64
		return s.Conn().QueryRow(s.t.Context(), `SELECT count(*) FROM `+dst).Scan(&n) == nil && n == 2
	})
	var inexact []int32
	rows, err := s.Conn().Query(s.t.Context(), fmt.Sprintf(`SELECT s.id FROM %s s JOIN %s d USING (id)
		WHERE s.d IS DISTINCT FROM d.d OR s.r IS DISTINCT FROM d.r OR s.da IS DISTINCT FROM d.da
			OR s.ra IS DISTINCT FROM d.ra ORDER BY 1`, src, dst))
	EnvNoError(s.t, env, err)
	for rows.Next() {
		var id int32
		EnvNoError(s.t, env, rows.Scan(&id))
		inexact = append(inexact, id)
	}
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
	require.Empty(s.t, inexact, "rows whose floats changed (1: snapshot, 2: CDC)")
}
