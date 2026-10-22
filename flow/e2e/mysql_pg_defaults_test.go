package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// NOT NULL and column defaults must reach the target, at table creation and on a mid-stream ADD COLUMN, with the
// mirror propagating nullability (PEERDB_NULLABLE). MySQL's ADD COLUMN ... DEFAULT fills existing rows without row
// events, so the target must fill them too.
func (s MySQLPGSuite) Test_Defaults_And_Not_Null_Carried() {
	t := s.t
	srcTable := "defaults_not_null"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		name VARCHAR(20) NOT NULL, qty INT NOT NULL DEFAULT 5, note VARCHAR(10) DEFAULT 'x')`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, name) VALUES (1, 'a'), (2, 'b')`, srcFull)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	if flowConnConfig.Env == nil {
		flowConnConfig.Env = map[string]string{}
	}
	flowConnConfig.Env["PEERDB_NULLABLE"] = "true"
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN added VARCHAR(10) NOT NULL DEFAULT 'dflt'`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, name, added) VALUES (3, 'c', 'given')`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc insert after ADD COLUMN", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 3`, dstFull)) == 1
	})

	// column -> "NOT NULL" / "" and its default expression ("" for none)
	rows, err := s.dest.Conn().Query(t.Context(), `SELECT a.attname, CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE '' END,
		coalesce(pg_get_expr(d.adbin, d.adrelid), '')
		FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped AND a.attname NOT LIKE '\_PEERDB\_%'`,
		dstFull)
	EnvNoError(t, env, err)
	columns := map[string][2]string{}
	for rows.Next() {
		var name, notNull, def string
		EnvNoError(t, env, rows.Scan(&name, &notNull, &def))
		columns[name] = [2]string{notNull, def}
	}
	EnvNoError(t, env, rows.Err())
	added := map[int32]string{}
	rows, err = s.dest.Conn().Query(t.Context(), fmt.Sprintf(`SELECT id, coalesce(added, 'NULL') FROM %s`, dstFull))
	EnvNoError(t, env, err)
	for rows.Next() {
		var id int32
		var v string
		EnvNoError(t, env, rows.Scan(&id, &v))
		added[id] = v
	}
	EnvNoError(t, env, rows.Err())
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, map[string][2]string{
		"id":    {"NOT NULL", ""},
		"name":  {"NOT NULL", ""},
		"qty":   {"NOT NULL", "5"},
		"note":  {"", "'x'::text"},
		"added": {"NOT NULL", "'dflt'::text"},
	}, columns)
	require.Equal(t, map[int32]string{1: "dflt", 2: "dflt", 3: "given"}, added)
}
