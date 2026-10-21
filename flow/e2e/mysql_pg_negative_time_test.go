package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// MySQL TIME is a duration (-838:59:59 to 838:59:59), so it goes into an interval target column (pre-created:
// PeerDB maps TIME to time, which can't hold these values). Negative values must keep their sign in every
// part, through the snapshot (rows 1-3) and CDC (row 4).
func (s MySQLPGSuite) Test_Negative_Time_Into_Interval() {
	t := s.t
	srcTable := "negative_time"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, c_time TIME, c_time6 TIME(6))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, '-838:59:59', '-838:59:59.000000'), (2, '-00:00:01', '-12:34:56.000001'), (3, '838:59:59', '100:00:00.5')`,
		srcFull)))
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id integer PRIMARY KEY,
		c_time interval, c_time6 interval, "_PEERDB_SYNCED_AT" timestamp DEFAULT CURRENT_TIMESTAMP)`, dstFull))
	require.NoError(t, err)

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
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (4, '-838:59:59', '-00:00:01.000001')`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc insert", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 4
	})

	rows, err := s.dest.Conn().Query(t.Context(),
		fmt.Sprintf(`SELECT id, c_time::text, c_time6::text FROM %s ORDER BY id`, dstFull))
	EnvNoError(t, env, err)
	got := map[int32][2]string{}
	for rows.Next() {
		var id int32
		var c, c6 string
		EnvNoError(t, env, rows.Scan(&id, &c, &c6))
		got[id] = [2]string{c, c6}
	}
	EnvNoError(t, env, rows.Err())
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, map[int32][2]string{
		1: {"-838:59:59", "-838:59:59"},
		2: {"-00:00:01", "-12:34:56.000001"},
		3: {"838:59:59", "100:00:00.5"},
		4: {"-838:59:59", "-00:00:01.000001"},
	}, got)
}
