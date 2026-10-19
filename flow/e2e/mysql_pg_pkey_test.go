package e2e

import (
	"fmt"
	"strconv"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Primary key changes beyond the basic case in Test_PKey_Update_Removes_Old_Row (finding #19): the
// destination must end up with exactly the source's rows. Fixed for mirrors from
// InternalVersion_MySQLPKeyChangeAsDeleteInsert on, which send a key change as delete + insert.
func (s MySQLPGSuite) Test_PKey_Change_Edge_Cases() {
	t := s.t
	edges, comp, part, ci := "pk_edges", "pk_comp", "pk_part", "pk_ci"
	exec := func(format string, table string, args ...any) {
		t.Helper()
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(format, append([]any{AttachSchema(s, table)}, args...)...)))
	}
	exec(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, edges)
	exec(`CREATE TABLE %s (a INT, b VARCHAR(10), v VARCHAR(20), PRIMARY KEY (a, b))`, comp)
	exec(`CREATE TABLE %s (id INT, created DATE, v VARCHAR(20), PRIMARY KEY (id, created))
		PARTITION BY RANGE (YEAR(created)) (PARTITION p2025 VALUES LESS THAN (2026), PARTITION pmax VALUES LESS THAN MAXVALUE)`, part)
	exec(`CREATE TABLE %s (code VARCHAR(10) COLLATE utf8mb4_general_ci PRIMARY KEY, v VARCHAR(20))`, ci)
	exec(`INSERT INTO %s VALUES (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')`, edges)
	exec(`INSERT INTO %s VALUES (1, 'x', 'a'), (1, 'y', 'b')`, comp)
	exec(`INSERT INTO %s VALUES (1, '2025-05-01', 'a'), (2, '2025-06-01', 'b')`, part)
	exec(`INSERT INTO %s VALUES ('abc', 'a'), ('xyz', 'b')`, ci)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, edges),
		TableMappings: TableMappings(s, edges, edges, comp, comp, part, part, ci, ci),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(edges))) == 4 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(ci))) == 2
	})

	cdc := func(format string, table string, args ...any) {
		t.Helper()
		EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(format, append([]any{AttachSchema(s, table)}, args...)...)))
	}
	// changed and changed back
	cdc(`UPDATE %s SET id = 103 WHERE id = 3`, edges)
	cdc(`UPDATE %s SET id = 3 WHERE id = 103`, edges)
	// swap 4 and 5 through a temporary key
	cdc(`UPDATE %s SET id = 1000 WHERE id = 4`, edges)
	cdc(`UPDATE %s SET id = 4 WHERE id = 5`, edges)
	cdc(`UPDATE %s SET id = 5 WHERE id = 1000`, edges)
	// old key reused by a new row
	cdc(`UPDATE %s SET id = 106 WHERE id = 6`, edges)
	cdc(`INSERT INTO %s VALUES (6, 'f-new')`, edges)
	// one column of a composite key
	cdc(`UPDATE %s SET b = 'z' WHERE a = 1 AND b = 'x'`, comp)
	// partition-key move
	cdc(`UPDATE %s SET created = '2026-05-01' WHERE id = 1`, part)
	// case-only change: the same key for MySQL's CI collation, a different one for PG text
	cdc(`UPDATE %s SET code = 'ABC' WHERE code = 'abc'`, ci)

	// expected destination contents: (rows matching the source, total rows)
	checks := []struct {
		what, query string
		want        int64
	}{
		{"pk_edges", `(id, v) IN ((3, 'c'), (4, 'e'), (5, 'd'), (6, 'f-new'), (106, 'f'))`, 5},
		{"pk_comp", `(a, b, v) IN ((1, 'y', 'b'), (1, 'z', 'a'))`, 2},
		{"pk_part", `(id, created, v) IN ((1, DATE '2026-05-01', 'a'), (2, DATE '2025-06-01', 'b'))`, 2},
		{"pk_ci", `(code, v) IN (('ABC', 'a'), ('xyz', 'b'))`, 2},
	}
	matching := func(i int) int64 {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, s.DestinationTable(checks[i].what), checks[i].query))
	}
	total := func(i int) int64 {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(checks[i].what)))
	}
	EnvWaitFor(t, env, 3*time.Minute, "source rows under their final keys", func() bool {
		for i := range checks {
			if matching(i) != checks[i].want {
				return false
			}
		}
		return true
	})
	totals := make([]int64, len(checks))
	for i := range checks {
		totals[i] = total(i)
	}

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	for i, c := range checks {
		require.EqualValues(t, c.want, totals[i], "%s: rows left under old primary keys (#19)", c.what)
	}
}

// Mirrors created before InternalVersion_MySQLPKeyChangeAsDeleteInsert keep sending key changes as
// updates (the old row stays on the destination), so an existing mirror's event stream doesn't change.
func (s MySQLPGSuite) Test_PKey_Change_Old_Version_Keeps_Update() {
	t := s.t
	srcTable := "pk_old"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, srcFull)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	version := shared.InternalVersion_MySQLPKeyChangeAsDeleteInsert - 1
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET id = 101 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "row under its new key", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 101`, dstFull)) == 1
	})
	oldKey := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 1`, dstFull))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.EqualValues(t, 1, oldKey, "old-version mirror: the row under the old key stays")
}
