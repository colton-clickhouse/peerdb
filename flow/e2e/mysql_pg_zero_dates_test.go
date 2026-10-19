package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Partial zero dates (MySQL -> Managed PG Pass 1 finding #11b): MySQL allows a zero month or day
// ('2024-00-15', '2024-02-00 12:00:00') where sql_mode permits; PG has no such dates. On v0.37.10 the reader
// rewrites "-00" to "-01", so they arrive as real, plausible dates that were never in the source
// ('2024-01-15', '2024-02-01 12:00:00'), in the snapshot and in CDC.
func (s MySQLPGSuite) Test_Partial_Zero_Dates_Not_Rolled_Forward() {
	t := s.t
	srcTable := "partial_zero_dates"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), `SET SESSION sql_mode = ''`))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_date DATE, c_datetime DATETIME, c_datetime6 DATETIME(6))`, srcFull)))
	// 1: partial zero dates, 2: zero dates (#11a, for comparison), 3: real dates (updated to partial in CDC)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, '2024-00-15', '2024-02-00 12:00:00', '2024-00-00 01:02:03.000004'),
		(2, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000'),
		(3, '2024-05-15', '2024-05-15 12:00:00', '2024-05-15 12:00:00.000001')`, srcFull)))

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
		`INSERT INTO %s VALUES (4, '2024-00-15', '2024-02-00 12:00:00', '2024-00-00 01:02:03.000004')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET c_date = '2024-00-15', c_datetime = '2024-02-00 12:00:00',
		 c_datetime6 = '2024-00-00 01:02:03.000004' WHERE id = 3`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc changes", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 4`, dstFull)) == 1 &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 3 AND c_date IS DISTINCT FROM DATE '2024-05-15'`,
				dstFull)) == 1
	})

	// what each row arrived as, and whether any partial zero date was rolled forward to a real date
	rolled := map[string]string{
		"c_date": "DATE '2024-01-15'", "c_datetime": "TIMESTAMP '2024-02-01 12:00:00'",
		"c_datetime6": "TIMESTAMP '2024-01-01 01:02:03.000004'",
	}
	var problems []string
	for _, c := range []struct {
		id   int
		what string
	}{{1, "snapshot"}, {4, "cdc insert"}, {3, "cdc update"}} {
		var date, dt, dt6 *string
		require.NoError(t, s.dest.Conn().QueryRow(t.Context(), fmt.Sprintf(
			`SELECT c_date::text, c_datetime::text, c_datetime6::text FROM %s WHERE id = %d`, dstFull, c.id)).Scan(&date, &dt, &dt6))
		t.Logf("#11b %s (id %d): c_date %v, c_datetime %v, c_datetime6 %v", c.what, c.id, deref(date), deref(dt), deref(dt6))
		for col, value := range rolled {
			if s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = %d AND %s = %s`, dstFull, c.id, col, value)) == 1 {
				problems = append(problems, fmt.Sprintf("%s (id %d): %s rolled forward to %s", c.what, c.id, col, value))
			}
		}
	}
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	for _, p := range problems {
		t.Log("#11b:", p)
	}
	require.Empty(t, problems, "partial zero dates must not become real dates (#11b)")
}

func deref(s *string) string {
	if s == nil {
		return "NULL"
	}
	return *s
}
