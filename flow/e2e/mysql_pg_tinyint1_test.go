package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// TINYINT(1) is read as a boolean, so values other than 0 and 1 arrive as true. A mirror can read every TINYINT(1)
// as an integer (PEERDB_MYSQL_TINYINT1_AS_INT), and a column setting's destination_type picks one column's
// decoding, winning over the mirror setting.
func (s MySQLPGSuite) Test_TinyInt1_As_Int_Mirror_Setting() {
	got := s.tinyInt1Flow("tinyint1_mirror", map[string]string{"PEERDB_MYSQL_TINYINT1_AS_INT": "true"},
		[]*protos.ColumnSetting{{SourceName: "b", DestinationType: "boolean"}})
	require.Equal(s.t, tinyInt1Result{
		types: [3]string{"smallint", "boolean", "smallint"},
		rows: map[int32][3]string{
			1: {"0", "false", "0"}, 2: {"3", "true", "1"}, 3: {"2", "true", "200"},
			4: {"127", "true", "255"}, 5: {"-5", "true", "7"},
		},
	}, got)
}

func (s MySQLPGSuite) Test_TinyInt1_As_Int_Column_Setting() {
	got := s.tinyInt1Flow("tinyint1_column", nil,
		[]*protos.ColumnSetting{{SourceName: "a", DestinationType: "smallint"}})
	require.Equal(s.t, tinyInt1Result{
		types: [3]string{"smallint", "boolean", "smallint"},
		rows: map[int32][3]string{
			1: {"0", "false", "0"}, 2: {"3", "true", "1"}, 3: {"2", "true", "200"},
			4: {"127", "true", "255"}, 5: {"-5", "true", "7"},
		},
	}, got)
}

type tinyInt1Result struct {
	rows  map[int32][3]string
	types [3]string
}

// tinyInt1Flow replicates a table with TINYINT(1) columns a, b and u (unsigned, a control: MySQL 8 drops its display
// width, so it's a plain TINYINT UNSIGNED, an integer either way): rows 1-4 through the snapshot,
// an update of row 2 and row 5 through CDC. It returns the destination's types and values (as text) for a, b, u.
func (s MySQLPGSuite) tinyInt1Flow(srcTable string, mirrorEnv map[string]string, columns []*protos.ColumnSetting) tinyInt1Result {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, a TINYINT(1), b TINYINT(1), u TINYINT(1) UNSIGNED)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, 0, 0, 0), (2, 1, 1, 1), (3, 2, -1, 200), (4, 127, -128, 255)`, srcFull)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.TableMappings[0].Columns = columns
	if flowConnConfig.Env == nil {
		flowConnConfig.Env = map[string]string{}
	}
	for k, v := range mirrorEnv {
		flowConnConfig.Env[k] = v
	}
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 4
	})
	// the update goes first: once the insert has arrived, so has the update
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET a = 3 WHERE id = 2`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (5, -5, 9, 7)`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc update and insert", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 5`, dstFull)) == 1
	})

	var res tinyInt1Result
	for i, col := range []string{"a", "b", "u"} {
		EnvNoError(t, env, s.dest.Conn().QueryRow(t.Context(),
			`SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = $1::regclass AND attname = $2`,
			dstFull, col).Scan(&res.types[i]))
	}
	rows, err := s.dest.Conn().Query(t.Context(), fmt.Sprintf(`SELECT id, a::text, b::text, u::text FROM %s`, dstFull))
	EnvNoError(t, env, err)
	res.rows = map[int32][3]string{}
	for rows.Next() {
		var id int32
		var v [3]string
		EnvNoError(t, env, rows.Scan(&id, &v[0], &v[1], &v[2]))
		res.rows[id] = v
	}
	EnvNoError(t, env, rows.Err())
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	return res
}
