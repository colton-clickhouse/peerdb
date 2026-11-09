//go:build tilt

package mysql_postgres

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
)

// A primary-key UPDATE during CDC must remove the row under its old key, not leave it next to the new
// one.
func (s MySQLPGSuite) Test_PKey_Update_Removes_Old_Row() {
	t := s.t
	srcTable := "pk_probe"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')`, srcFull)))

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
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, srcTable, "id,v")

	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 101 WHERE id = 1`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 102, v = 'b2' WHERE id = 2`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'c2' WHERE id = 3`, srcFull)))

	e2e.EnvWaitFor(t, env, 3*time.Minute, "updated rows under their new keys", func() bool {
		return s.destInt(fmt.Sprintf(
			`SELECT count(*) FROM %s WHERE (id, v) IN ((101, 'a'), (102, 'b2'), (3, 'c2'))`, dstFull)) == 3
	})
	oldKeys := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (1, 2)`, dstFull))
	total := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull))

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)

	require.EqualValues(t, 0, oldKeys, "rows under the old primary keys were left behind")
	require.EqualValues(t, 3, total, "destination row count")
}

// Primary key changes beyond the basic case in Test_PKey_Update_Removes_Old_Row: the destination must end up
// with exactly the source's rows, nothing left under an old key.
func (s MySQLPGSuite) Test_PKey_Change_Edge_Cases() {
	t := s.t
	edges, comp, ci := "pk_edges", "pk_comp", "pk_ci"
	exec := func(format string, table string, args ...any) error {
		return s.source.Exec(t.Context(), fmt.Sprintf(format, append([]any{e2e.AttachSchema(s, table)}, args...)...))
	}
	count := func(table string, cond string) int64 {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, s.DestinationTable(table), cond))
	}
	require.NoError(t, exec(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, edges))
	require.NoError(t, exec(`CREATE TABLE %s (a INT, b VARCHAR(10), v VARCHAR(20), PRIMARY KEY (a, b))`, comp))
	require.NoError(t, exec(`CREATE TABLE %s (code VARCHAR(10) COLLATE utf8mb4_general_ci PRIMARY KEY, v VARCHAR(20))`, ci))
	require.NoError(t, exec(`INSERT INTO %s VALUES (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')`, edges))
	require.NoError(t, exec(`INSERT INTO %s VALUES (1, 'x', 'a'), (1, 'y', 'b')`, comp))
	require.NoError(t, exec(`INSERT INTO %s VALUES ('abc', 'a'), ('xyz', 'b')`, ci))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, edges),
		TableMappings: e2e.TableMappings(s, edges, edges, comp, comp, ci, ci),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return count(edges, "true") == 4 && count(comp, "true") == 2 && count(ci, "true") == 2
	})

	// changed and changed back
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET id = 103 WHERE id = 3`, edges))
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET id = 3 WHERE id = 103`, edges))
	// swap 4 and 5 through a temporary key
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET id = 1000 WHERE id = 4`, edges))
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET id = 4 WHERE id = 5`, edges))
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET id = 5 WHERE id = 1000`, edges))
	// old key reused by a new row
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET id = 106 WHERE id = 6`, edges))
	e2e.EnvNoError(t, env, exec(`INSERT INTO %s VALUES (6, 'f-new')`, edges))
	// one column of a composite key
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET b = 'z' WHERE a = 1 AND b = 'x'`, comp))
	// case-only change: the same key for MySQL's CI collation, a different one for PG text
	e2e.EnvNoError(t, env, exec(`UPDATE %s SET code = 'ABC' WHERE code = 'abc'`, ci))

	// the source's rows under their final keys, and how many rows each table must have in all
	want := []struct {
		table, rows string
		n           int64
	}{
		{edges, `(id, v) IN ((3, 'c'), (4, 'e'), (5, 'd'), (6, 'f-new'), (106, 'f'))`, 5},
		{comp, `(a, b, v) IN ((1, 'y', 'b'), (1, 'z', 'a'))`, 2},
		{ci, `(code, v) IN (('ABC', 'a'), ('xyz', 'b'))`, 2},
	}
	e2e.EnvWaitFor(t, env, 3*time.Minute, "source rows under their final keys", func() bool {
		for _, w := range want {
			if count(w.table, w.rows) != w.n {
				return false
			}
		}
		return true
	})
	var left []string
	for _, w := range want {
		if n := count(w.table, "true"); n != w.n {
			left = append(left, fmt.Sprintf("%s: %d rows, want %d", w.table, n, w.n))
		}
	}
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)

	require.Empty(t, left, "rows left under old primary keys")
}
