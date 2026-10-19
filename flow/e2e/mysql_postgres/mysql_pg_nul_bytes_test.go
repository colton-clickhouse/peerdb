//go:build tilt

package mysql_postgres

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
)

// NUL bytes in strings: PG text can't hold them, PeerDB strips them.
func (s MySQLPGSuite) Test_Nul_Bytes_Stripped() {
	t := s.t
	srcTable := "nul_strip"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, s VARCHAR(20), tx TEXT)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, CONCAT('a', CHAR(0), 'b'), CHAR(0)), (2, 'plain', 't')`, srcFull)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (11, CONCAT('a', CHAR(0), 'b'), CHAR(0))`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET s = CONCAT(CHAR(0), 'c') WHERE id = 2`, srcFull)))
	e2e.EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id IN (1, 11) AND s = 'ab' AND tx = '') OR (id = 2 AND s = 'c')`,
			dstFull)) == 3
	})
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
