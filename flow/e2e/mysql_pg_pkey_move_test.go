package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// pkeyMoveFlow mirrors a table (id INT PRIMARY KEY, v VARCHAR(20)) holding rows, and waits for the initial load
func (s MySQLPGSuite) pkeyMoveFlow(srcTable string, softDelete bool, rows string, count int64) WorkflowRun {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s`, srcFull, rows)))
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
		SoftDelete:    softDelete,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	env := ExecutePeerflow(t, NewTemporalClient(t), flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(srcTable))) == count
	})
	return env
}

// With soft delete, a key change marks the row under the old key deleted and inserts the new one; a row inserted
// later under the old key replaces the deleted one.
func (s MySQLPGSuite) Test_PKey_Change_Soft_Delete() {
	t := s.t
	srcTable := "pk_move_soft_delete"
	srcFull, dstFull := AttachSchema(s, srcTable), s.DestinationTable(srcTable)
	env := s.pkeyMoveFlow(srcTable, true, `(1, 'a'), (2, 'b')`, 2)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 101 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "old key soft-deleted, new key inserted", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE
			(id = 1 AND v = 'a' AND "_PEERDB_IS_DELETED") OR (id = 101 AND v = 'a' AND NOT "_PEERDB_IS_DELETED")`,
			dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'reused')`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "old key reused", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 1 AND v = 'reused' AND NOT "_PEERDB_IS_DELETED"`,
			dstFull)) == 1
	})
	require.EqualValues(t, 3, s.destInt(`SELECT count(*) FROM `+dstFull))
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// One statement shifting every key up by one, highest first: each row's new key is the old key of the row
// moved just before it, all in one binlog event and one batch.
func (s MySQLPGSuite) Test_PKey_Shift_Multi_Row() {
	t := s.t
	srcTable := "pk_shift"
	srcFull, dstFull := AttachSchema(s, srcTable), s.DestinationTable(srcTable)
	env := s.pkeyMoveFlow(srcTable, false, `(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')`, 5)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = id + 1 ORDER BY id DESC`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "every key shifted", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id, v) IN
			((2, 'a'), (3, 'b'), (4, 'c'), (5, 'd'), (6, 'e'))`, dstFull)) == 5
	})
	require.EqualValues(t, 5, s.destInt(`SELECT count(*) FROM `+dstFull), "no row left under an old key")
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
