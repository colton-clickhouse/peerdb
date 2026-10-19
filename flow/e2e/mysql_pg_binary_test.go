package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Empty (zero-length, non-NULL) binary values must stay empty on the destination, in the snapshot and
// in CDC. Repro for MySQL -> Managed PG Pass 1 finding #10: on v0.37.10 empty BLOBs become NULL in the
// snapshot, and empty VARBINARY / CHARACTER SET binary strings become NULL in CDC. Fixed for mirrors
// from InternalVersion_MySQLEmptyBinaryAsEmpty on.
func (s MySQLPGSuite) Test_Empty_Binary_Stays_Empty() {
	problems := s.emptyBinaryProblems("empty_binary", shared.InternalVersion_Latest)
	for _, p := range problems {
		s.t.Log("#10:", p)
	}
	require.Empty(s.t, problems, "empty binary values must arrive as empty, not NULL (#10)")
}

// Mirrors created before InternalVersion_MySQLEmptyBinaryAsEmpty keep writing NULL for empty binary
// values, so a running mirror doesn't switch from NULL to empty mid-stream.
func (s MySQLPGSuite) Test_Empty_Binary_Old_Version_Keeps_NULL() {
	problems := s.emptyBinaryProblems("empty_binary_old", shared.InternalVersion_MySQLEmptyBinaryAsEmpty-1)
	for _, p := range problems {
		s.t.Log("old version:", p)
	}
	// the deterministic cases: the first snapshot row (go-mysql's buffers still nil) and CDC strings
	require.Contains(s.t, problems,
		"snapshot, empty row (id 1): c_tinyblob NULL, c_blob NULL, c_mediumblob NULL, c_longblob NULL, c_varbinary NULL, c_binary_cs NULL")
	require.Contains(s.t, problems, "cdc insert (id 4): c_varbinary NULL, c_binary_cs NULL")
}

// emptyBinaryProblems replicates empty, NULL and non-empty binary values through a mirror created at
// version, and returns the empty values that arrived NULL or non-empty. NULLs must stay NULL either way.
func (s MySQLPGSuite) emptyBinaryProblems(srcTable string, version uint32) []string {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	cols := []string{"c_tinyblob", "c_blob", "c_mediumblob", "c_longblob", "c_varbinary", "c_binary_cs"}
	// eachCol renders format once per column (%s is the column name; a format without %s is repeated
	// as is, e.g. "NULL") and joins the results
	eachCol := func(format string, sep string) string {
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = format
			if strings.Contains(format, "%s") {
				parts[i] = fmt.Sprintf(format, c)
			}
		}
		return strings.Join(parts, sep)
	}

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id INT PRIMARY KEY, touch INT NOT NULL DEFAULT 0,
		c_tinyblob TINYBLOB, c_blob BLOB, c_mediumblob MEDIUMBLOB, c_longblob LONGBLOB,
		c_varbinary VARBINARY(16), c_binary_cs VARCHAR(16) CHARACTER SET binary)`, srcFull)))
	// 1: all empty, 2: all NULL, 3: all non-empty (updated to empty during CDC), 5: all empty, read after
	// a non-empty row (go-mysql reuses per-column buffers, so the snapshot result depends on row order)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, %s) VALUES (1, %s), (2, %s), (3, %s), (5, %s)`, srcFull, eachCol("%s", ", "),
		eachCol("''", ", "), eachCol("NULL", ", "), eachCol("0x01", ", "), eachCol("''", ", "))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 4
	})

	// columns of row id that are NULL or non-empty on the destination (should be none for empty rows)
	wrong := func(id int) []string {
		var out []string
		for _, c := range cols {
			switch {
			case s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = %d AND %s IS NULL`, dstFull, id, c)) == 1:
				out = append(out, c+" NULL")
			case s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = %d AND octet_length(%s) > 0`, dstFull, id, c)) == 1:
				out = append(out, c+" non-empty")
			}
		}
		return out
	}
	var problems []string
	for _, c := range []struct {
		id   int
		what string
	}{{1, "snapshot, empty row"}, {5, "snapshot, empty row after a non-empty one"}} {
		if w := wrong(c.id); len(w) > 0 {
			problems = append(problems, fmt.Sprintf("%s (id %d): %s", c.what, c.id, strings.Join(w, ", ")))
		}
	}
	nulls := s.destInt(fmt.Sprintf(`SELECT %s FROM %s WHERE id = 2`, eachCol("(%s IS NULL)::int", " + "), dstFull))
	require.EqualValues(t, len(cols), nulls, "snapshot, NULL row (id 2): NULLs must stay NULL")

	// CDC: a new empty row, a row updated to empty, and the empty snapshot row re-sent by an unrelated update
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, touch, %s) VALUES (4, 1, %s)`,
		srcFull, eachCol("%s", ", "), eachCol("''", ", "))))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET touch = 1, %s WHERE id = 3`,
		srcFull, eachCol("%s = ''", ", "))))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET touch = 1 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc changes", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE touch = 1`, dstFull)) == 3
	})
	for _, c := range []struct {
		id   int
		what string
	}{{4, "cdc insert"}, {3, "cdc update to empty"}, {1, "cdc re-sent empty row"}} {
		if w := wrong(c.id); len(w) > 0 {
			problems = append(problems, fmt.Sprintf("%s (id %d): %s", c.what, c.id, strings.Join(w, ", ")))
		}
	}
	nulls = s.destInt(fmt.Sprintf(`SELECT %s FROM %s WHERE id = 2`, eachCol("(%s IS NULL)::int", " + "), dstFull))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.EqualValues(t, len(cols), nulls, "after cdc, NULL row (id 2): NULLs must stay NULL")
	return problems
}
