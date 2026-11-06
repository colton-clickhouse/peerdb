package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// MySQL's ADD COLUMN ... DEFAULT fills the existing rows without row events, so the target must fill them too
// with the same constant. Rows changed just before the ALTER (normalized with it or after) hold it as well. The
// default only fills: the target column keeps no default (column defaults are a cutover step). A time default
// isn't carried: it would fill the time of the replay, not the source's.
func (s MySQLPGSuite) Test_Add_Column_Default_Fills_Existing_Rows() {
	t := s.t
	srcTable := "add_column_default"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, name) VALUES (1, 'a'), (2, 'b')`, srcFull)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	env := ExecutePeerflow(t, NewTemporalClient(t), flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	// changes captured just before the ALTER, normalized with or after it: an insert and an update
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (4, 'd')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET name = 'b2' WHERE id = 2`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN added VARCHAR(10) NOT NULL DEFAULT 'dflt', ADD COLUMN qty INT DEFAULT 7`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN added_at DATETIME DEFAULT CURRENT_TIMESTAMP`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, name, added, qty) VALUES (3, 'c', 'given', 8)`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc insert after ADD COLUMN", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (3, 4)`, dstFull)) == 2
	})

	var defaults int64
	err := s.dest.Conn().QueryRow(t.Context(), `SELECT count(*) FROM pg_attrdef d
		JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
		WHERE d.adrelid = $1::regclass AND a.attname IN ('added', 'qty', 'added_at')`, dstFull).Scan(&defaults)
	EnvNoError(t, env, err)
	values := map[int32]string{}
	rows, err := s.dest.Conn().Query(t.Context(), fmt.Sprintf(
		`SELECT id, coalesce(added, 'NULL') || '|' || coalesce(qty::text, 'NULL') FROM %s`, dstFull))
	EnvNoError(t, env, err)
	for rows.Next() {
		var id int32
		var v string
		EnvNoError(t, env, rows.Scan(&id, &v))
		values[id] = v
	}
	EnvNoError(t, env, rows.Err())
	timeFilled := s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id <> 3 AND added_at IS NOT NULL`, dstFull))
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	// rows 2 (updated) and 4 (inserted) were changed before the ALTER: they hold the default too
	require.Equal(t, map[int32]string{1: "dflt|7", 2: "dflt|7", 3: "given|8", 4: "dflt|7"}, values)
	require.Zero(t, defaults, "the default only fills the rows that predate the column")
	require.Zero(t, timeFilled, "rows that predate a time default stay NULL")
}
