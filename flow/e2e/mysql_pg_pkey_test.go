package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Primary key changes beyond the basic case in Test_PKey_Update_Removes_Old_Row: the destination must end up
// with exactly the source's rows, nothing left under an old key.
func (s MySQLPGSuite) Test_PKey_Change_Edge_Cases() {
	t := s.t
	edges, comp, ci := "pk_edges", "pk_comp", "pk_ci"
	exec := func(format string, table string, args ...any) error {
		return s.source.Exec(t.Context(), fmt.Sprintf(format, append([]any{AttachSchema(s, table)}, args...)...))
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, edges),
		TableMappings: TableMappings(s, edges, edges, comp, comp, ci, ci),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return count(edges, "true") == 4 && count(comp, "true") == 2 && count(ci, "true") == 2
	})

	// changed and changed back
	EnvNoError(t, env, exec(`UPDATE %s SET id = 103 WHERE id = 3`, edges))
	EnvNoError(t, env, exec(`UPDATE %s SET id = 3 WHERE id = 103`, edges))
	// swap 4 and 5 through a temporary key
	EnvNoError(t, env, exec(`UPDATE %s SET id = 1000 WHERE id = 4`, edges))
	EnvNoError(t, env, exec(`UPDATE %s SET id = 4 WHERE id = 5`, edges))
	EnvNoError(t, env, exec(`UPDATE %s SET id = 5 WHERE id = 1000`, edges))
	// old key reused by a new row
	EnvNoError(t, env, exec(`UPDATE %s SET id = 106 WHERE id = 6`, edges))
	EnvNoError(t, env, exec(`INSERT INTO %s VALUES (6, 'f-new')`, edges))
	// one column of a composite key
	EnvNoError(t, env, exec(`UPDATE %s SET b = 'z' WHERE a = 1 AND b = 'x'`, comp))
	// case-only change: the same key for MySQL's CI collation, a different one for PG text
	EnvNoError(t, env, exec(`UPDATE %s SET code = 'ABC' WHERE code = 'abc'`, ci))

	// the source's rows under their final keys, and how many rows each table must have in all
	want := []struct {
		table, rows string
		n           int64
	}{
		{edges, `(id, v) IN ((3, 'c'), (4, 'e'), (5, 'd'), (6, 'f-new'), (106, 'f'))`, 5},
		{comp, `(a, b, v) IN ((1, 'y', 'b'), (1, 'z', 'a'))`, 2},
		{ci, `(code, v) IN (('ABC', 'a'), ('xyz', 'b'))`, 2},
	}
	EnvWaitFor(t, env, 3*time.Minute, "source rows under their final keys", func() bool {
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
	RequireEnvCanceled(t, env)

	require.Empty(t, left, "rows left under old primary keys")
}
