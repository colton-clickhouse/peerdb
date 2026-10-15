package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

// Empty (zero-length, non-NULL) binary values must stay empty on the destination, in the snapshot and
// in CDC. Repro for MySQL -> Managed PG Pass 1 finding #10: on v0.37.10 empty BLOBs become NULL in the
// snapshot, and empty VARBINARY / CHARACTER SET binary strings become NULL in CDC.
func (s MySQLPGSuite) Test_Empty_Binary_Stays_Empty() {
	t := s.t
	srcTable := "empty_binary"
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
	// 1: all empty, 2: all NULL, 3: all non-empty (updated to empty during CDC)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, %s) VALUES (1, %s), (2, %s), (3, %s)`,
		srcFull, eachCol("%s", ", "), eachCol("''", ", "), eachCol("NULL", ", "), eachCol("0x01", ", "))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 3
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
	if w := wrong(1); len(w) > 0 {
		problems = append(problems, "snapshot, empty row (id 1): "+strings.Join(w, ", "))
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

	for _, p := range problems {
		t.Log("#10:", p)
	}
	require.EqualValues(t, len(cols), nulls, "after cdc, NULL row (id 2): NULLs must stay NULL")
	require.Empty(t, problems, "empty binary values must arrive as empty, not NULL (#10)")
}
