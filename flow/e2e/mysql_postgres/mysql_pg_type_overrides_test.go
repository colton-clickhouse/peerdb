//go:build tilt

package mysql_postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// A column setting's destination_type picks the Postgres column's type, for mappings that can't hold the source's
// values: INT UNSIGNED above integer, BIGINT UNSIGNED above bigint, TIME as a duration, and TINYINT(1) holding small
// integers (read as an integer with an integer destination_type, not as a boolean). The snapshot and CDC must carry
// every value.
func (s MySQLPGSuite) Test_Column_Type_Overrides() {
	t := s.t
	srcTable := "type_overrides"
	srcFull, dstFull := e2e.AttachSchema(s, srcTable), s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		u INT UNSIGNED, b BIGINT UNSIGNED, d TIME(1), f TINYINT(1), keep TINYINT(1))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, 4294967295, 18446744073709551615, '-838:59:59.0', 127, 1), (2, 0, 0, '100:00:00.5', -5, 0)`,
		srcFull)))

	tableMappings := e2e.TableMappings(s, srcTable, srcTable)
	tableMappings[0].Columns = []*protos.ColumnSetting{
		{SourceName: "u", DestinationType: "bigint"},
		{SourceName: "b", DestinationType: "numeric(20,0)"},
		{SourceName: "d", DestinationType: "interval"},
		{SourceName: "f", DestinationType: "smallint"},
	}
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: tableMappings,
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	env := e2e.ExecutePeerflow(t, e2e.NewTemporalClient(t), flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(`SELECT count(*) FROM `+dstFull) == 2
	})
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (3, 4294967294, 18446744073709551614, '-00:00:01.5', 2, 1)`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET f = -128 WHERE id = 2`, srcFull)))
	e2e.EnvWaitFor(t, env, 2*time.Minute, "CDC rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 3 OR (id = 2 AND f = -128)`, dstFull)) == 2
	})

	types := map[string]string{}
	rows, err := s.dest.Conn().Query(t.Context(), `SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped`, dstFull)
	e2e.EnvNoError(t, env, err)
	for rows.Next() {
		var name, typ string
		e2e.EnvNoError(t, env, rows.Scan(&name, &typ))
		types[name] = typ
	}
	e2e.EnvNoError(t, env, rows.Err())
	got := map[int32]string{}
	rows, err = s.dest.Conn().Query(t.Context(), fmt.Sprintf(
		`SELECT id, concat_ws('|', u, b, d, f, keep) FROM %s ORDER BY id`, dstFull))
	e2e.EnvNoError(t, env, err)
	for rows.Next() {
		var id int32
		var v string
		e2e.EnvNoError(t, env, rows.Scan(&id, &v))
		got[id] = v
	}
	e2e.EnvNoError(t, env, rows.Err())
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)

	for col, want := range map[string]string{
		"u": "bigint", "b": "numeric(20,0)", "d": "interval", "f": "smallint", "keep": "boolean",
	} {
		require.Equal(t, want, types[col], col)
	}
	require.Equal(t, map[int32]string{
		1: "4294967295|18446744073709551615|-838:59:59|127|t",
		2: "0|0|100:00:00.5|-128|f",
		3: "4294967294|18446744073709551614|-00:00:01.5|2|t",
	}, got)
}

// destination_type is refused at validation where Postgres couldn't hold or write the values: a choice outside
// the column's list, and a type the destination doesn't have.
func (s MySQLPGSuite) Test_Column_Type_Overrides_Refused() {
	t := s.t
	srcTable := "type_overrides_refused"
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, d TIME, v VARCHAR(10))`, e2e.AttachSchema(s, srcTable))))
	client, err := e2e.NewApiClient()
	require.NoError(t, err)
	validate := func(settings ...*protos.ColumnSetting) error {
		t.Helper()
		tableMappings := e2e.TableMappings(s, srcTable, srcTable)
		tableMappings[0].Columns = settings
		flowConnConfig := (&e2e.FlowConnectionGenerationConfig{
			FlowJobName: e2e.AddSuffix(s, srcTable), TableMappings: tableMappings, Destination: s.Peer().Name,
		}).GenerateFlowConnectionConfigs(s)
		flowConnConfig.DoInitialSnapshot = true
		_, err := client.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
		return err
	}

	require.NoError(t, validate(&protos.ColumnSetting{SourceName: "d", DestinationType: "interval"}))
	err = validate(&protos.ColumnSetting{SourceName: "d", DestinationType: "bigint"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "isn't a destination_type for its type")
	// citext is an extension: refused where it isn't installed, accepted where it is
	var citext bool
	require.NoError(t, s.dest.Conn().QueryRow(t.Context(), `SELECT to_regtype('citext') IS NOT NULL`).Scan(&citext))
	err = validate(&protos.ColumnSetting{SourceName: "v", DestinationType: "citext"})
	if citext {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "doesn't exist on the destination"), err.Error())
	}
}
