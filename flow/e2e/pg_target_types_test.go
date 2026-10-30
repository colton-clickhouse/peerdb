package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Postgres -> Postgres on the Q type system into pre-created targets of other types: CDC casts to the target's
// types, with arrays and floats going through PeerDB's own types (text[] has no cast to integer[]; a real is
// written as the snapshot writes it) and text cast directly (enum, jsonb, varchar(n)).
func (s PeerFlowE2ETestSuitePG) Test_Q_CDC_Into_Precreated_Target_Types() {
	tc := NewTemporalClient(s.t)
	src := s.attachSchemaSuffix("q_target_types")
	dst := s.attachSchemaSuffix("q_target_types_dst")
	mood := s.attachSchemaSuffix("q_mood")
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s (id int PRIMARY KEY, dbls double precision[], mood text, doc json, code text, f real);
		CREATE TYPE %[3]s AS ENUM ('sad', 'happy');
		CREATE TABLE %[2]s (id int PRIMARY KEY, dbls integer[], mood %[3]s, doc jsonb, code varchar(5),
			f double precision)`, src, dst, mood))
	require.NoError(s.t, err)

	flowConnConfig := (&FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("q_target_types"),
		TableNameMapping: map[string]string{src: dst},
		Destination:      s.Peer().Name,
	}).GenerateFlowConnectionConfigs(s)
	flowConnConfig.System = protos.TypeSystem_Q
	flowConnConfig.SyncedAtColName = ""
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, '{1.5,2.5,3.7}', 'happy', '{"b": 2, "a": 1}', 'abc', 0.1)`, src))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 2*time.Minute, "row through CDC", func() bool {
		var n int64
		err := s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 1
			AND dbls = '{2,2,4}' AND mood = 'happy' AND doc = '{"a": 1, "b": 2}'::jsonb AND code = 'abc'
			AND f = 0.1::real::double precision`, dst)).Scan(&n)
		return err == nil && n == 1
	})
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
