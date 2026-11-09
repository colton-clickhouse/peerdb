//go:build tilt

package mysql_postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
)

// Empty (zero-length, non-NULL) binary values must stay empty on the destination, in the snapshot and in
// CDC, and NULLs must stay NULL. The stock table comparison treats NULL and empty bytes as equal, so this
// checks with direct SQL.
func (s MySQLPGSuite) Test_Empty_Binary_Stays_Empty() {
	t := s.t
	srcTable := "empty_binary"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	cols := []string{"c_tinyblob", "c_blob", "c_mediumblob", "c_longblob", "c_varbinary", "c_binary_cs"}
	allEmpty := make([]string, len(cols))
	allNull := make([]string, len(cols))
	for i, c := range cols {
		allEmpty[i] = fmt.Sprintf("octet_length(%s) = 0", c)
		allNull[i] = c + " IS NULL"
	}
	countWhere := func(cond string) int64 {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, dstFull, cond))
	}

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, touch INT NOT NULL DEFAULT 0,
		c_tinyblob TINYBLOB, c_blob BLOB, c_mediumblob MEDIUMBLOB, c_longblob LONGBLOB,
		c_varbinary VARBINARY(16), c_binary_cs VARCHAR(16) CHARACTER SET binary)`, srcFull)))
	// 1: empty, 2: NULL, 3: non-empty (updated to empty during CDC), 5: empty after a non-empty row
	// (go-mysql reuses per-column buffers, so the snapshot result depends on row order)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, %s) VALUES
		(1, '', '', '', '', '', ''), (2, NULL, NULL, NULL, NULL, NULL, NULL),
		(3, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01), (5, '', '', '', '', '', '')`, srcFull, strings.Join(cols, ", "))))

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
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return countWhere("true") == 4
	})
	require.EqualValues(t, 2, countWhere("id IN (1, 5) AND "+strings.Join(allEmpty, " AND ")), "snapshot: empty stays empty")

	// CDC: a new empty row, a row updated to empty, and the empty snapshot row re-sent by an unrelated update
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, touch, %s) VALUES (4, 1, '', '', '', '', '', '')`, srcFull, strings.Join(cols, ", "))))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET touch = 1, c_tinyblob = '', c_blob = '',
		c_mediumblob = '', c_longblob = '', c_varbinary = '', c_binary_cs = '' WHERE id = 3`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET touch = 1 WHERE id IN (1, 2)`, srcFull)))
	e2e.EnvWaitFor(t, env, 3*time.Minute, "cdc changes", func() bool {
		return countWhere("touch = 1") == 4
	})
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)

	require.EqualValues(t, 4, countWhere("id IN (1, 3, 4, 5) AND "+strings.Join(allEmpty, " AND ")), "cdc: empty stays empty")
	require.EqualValues(t, 1, countWhere("id = 2 AND "+strings.Join(allNull, " AND ")), "NULL stays NULL")
}
