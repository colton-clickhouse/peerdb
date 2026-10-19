package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Partial zero dates (MySQL -> Managed PG Pass 1 finding #11b): MySQL allows a zero month or day
// ('2024-00-15', '2024-02-00 12:00:00') where sql_mode permits; PG has no such dates. On v0.37.10 the reader
// rewrites "-00" to "-01", so they arrive as real, plausible dates that were never in the source
// ('2024-01-15', '2024-02-01 12:00:00'), in the snapshot and in CDC. From
// InternalVersion_MySQLZeroDatePartsAsZero on they're replicated like zero dates (#11a: the epoch), and so
// is a zero year ('0000-05-10'), which PG can't store either.
func (s MySQLPGSuite) Test_Partial_Zero_Dates_As_Zero_Dates() {
	// rows: 1 partial (snapshot), 2 zero (snapshot, #11a), 3 real -> partial (cdc update), 4 partial (cdc
	// insert), 5 zero year (snapshot), 6 zero year (cdc insert)
	got := s.zeroDateRows("partial_zero_dates", shared.InternalVersion_Latest, true)
	epoch := "1970-01-01 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00"
	for _, id := range []int{1, 2, 3, 4, 5, 6} {
		require.Equal(s.t, epoch, got[id], "id %d: zero, partial zero and zero-year dates must arrive as the epoch (#11a/#11b)", id)
	}
}

// Mirrors created before InternalVersion_MySQLZeroDatePartsAsZero keep rolling a zero month or day forward,
// so an existing mirror's values don't change mid-stream. (No zero-year rows: those fail older mirrors.)
func (s MySQLPGSuite) Test_Partial_Zero_Dates_Old_Version_Rolled_Forward() {
	got := s.zeroDateRows("partial_zero_dates_old", shared.InternalVersion_MySQLZeroDatePartsAsZero-1, false)
	rolled := "2024-01-15 | 2024-02-01 12:00:00 | 2024-01-01 01:02:03.000004"
	for _, id := range []int{1, 3, 4} {
		require.Equal(s.t, rolled, got[id], "id %d: old-version mirror", id)
	}
	require.Equal(s.t, "1970-01-01 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00", got[2], "zero dates")
}

// zeroDateRows replicates zero, partial zero (and, with zeroYears, zero-year) dates through the snapshot and
// CDC with a mirror at version, and returns each row's destination values as "date | datetime | datetime6".
func (s MySQLPGSuite) zeroDateRows(srcTable string, version uint32, zeroYears bool) map[int]string {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	partial := `'2024-00-15', '2024-02-00 12:00:00', '2024-00-00 01:02:03.000004'`
	zeroYear := `'0000-05-10', '0000-02-03 04:05:06', '0000-02-03 04:05:06.000007'`
	require.NoError(t, s.source.Exec(t.Context(), `SET SESSION sql_mode = ''`))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY,
		c_date DATE, c_datetime DATETIME, c_datetime6 DATETIME(6))`, srcFull)))
	rows := []string{
		"(1, " + partial + ")",
		"(2, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000')",
		"(3, '2024-05-15', '2024-05-15 12:00:00', '2024-05-15 12:00:00.000001')",
	}
	if zeroYears {
		rows = append(rows, "(5, "+zeroYear+")")
	}
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s`, srcFull, strings.Join(rows, ", "))))

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
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == int64(len(rows))
	})

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (4, %s)`, srcFull, partial)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET c_date = '2024-00-15', c_datetime = '2024-02-00 12:00:00',
		 c_datetime6 = '2024-00-00 01:02:03.000004' WHERE id = 3`, srcFull)))
	want := len(rows) + 1
	if zeroYears {
		EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (6, %s)`, srcFull, zeroYear)))
		want++
	}
	EnvWaitFor(t, env, 3*time.Minute, "cdc changes", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == int64(want) &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 3 AND c_date IS DISTINCT FROM DATE '2024-05-15'`,
				dstFull)) == 1
	})

	got := map[int]string{}
	rs, err := s.dest.Conn().Query(t.Context(), fmt.Sprintf(
		`SELECT id, concat_ws(' | ', coalesce(c_date::text, 'NULL'), coalesce(c_datetime::text, 'NULL'),
		 coalesce(c_datetime6::text, 'NULL')) FROM %s`, dstFull))
	require.NoError(t, err)
	for rs.Next() {
		var id int
		var v string
		require.NoError(t, rs.Scan(&id, &v))
		got[id] = v
		t.Logf("#11b id %d: %s", id, v)
	}
	require.NoError(t, rs.Err())
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	return got
}
