package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// MySQL TIME is a duration (-838:59:59 to 838:59:59), so PeerDB must create its Postgres column as interval:
// time (a time of day, 00:00 to 24:00) can't hold negative or over-24h values, and the snapshot fails on them
// ("time out of range") and retries forever. No pre-created target here, unlike Test_Negative_Time_Into_Interval.
func (s MySQLPGSuite) Test_Time_Created_As_Interval() {
	t := s.t
	srcTable := "time_interval"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, c_time TIME, c_time6 TIME(6))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, '-838:59:59', '838:59:59.000000'), (2, '24:00:00', '-00:00:00.000001'), (3, '12:34:56', '00:00:00.5')`,
		srcFull)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "destination table created", func() bool {
		return len(s.destColumnTypes(srcTable)) > 0
	})
	types := s.destColumnTypes(srcTable)
	if types["c_time"] != "interval" || types["c_time6"] != "interval" {
		env.Cancel(t.Context())
		RequireEnvCanceled(t, env)
		require.Failf(t, "TIME columns not created as interval", "c_time %s, c_time6 %s", types["c_time"], types["c_time6"])
	}
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 3
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (4, '-838:59:59', '-100:00:00.000001')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET c_time = '100:00:00' WHERE id = 3`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc insert and update", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 4 OR c_time = '100:00:00'`, dstFull)) == 2
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
		1: {"-838:59:59", "838:59:59"},
		2: {"24:00:00", "-00:00:00.000001"},
		3: {"100:00:00", "00:00:00.5"},
		4: {"-838:59:59", "-100:00:00.000001"},
	}, got)
}
