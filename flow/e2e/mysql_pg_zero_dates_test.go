package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Partial zero dates: MySQL allows a zero month or day ('2024-00-15', '2024-02-00 12:00:00') where sql_mode
// permits; Postgres has no such dates. The reader used to rewrite "-00" to "-01", so they arrived as real,
// plausible dates that were never in the source ('2024-01-15', '2024-02-01 12:00:00'), in the snapshot and in
// CDC. They must arrive like zero dates (the epoch), and so must a zero year ('0000-05-10').
func (s MySQLPGSuite) Test_Partial_Zero_Dates_As_Zero_Dates() {
	// rows: 1 partial (snapshot), 2 zero (snapshot), 3 real -> partial (cdc update), 4 partial (cdc
	// insert), 5 zero year (snapshot), 6 zero year (cdc insert)
	got := s.zeroDateRows("partial_zero_dates", shared.InternalVersion_Latest, true)
	epoch := "1970-01-01 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00"
	for _, id := range []int{1, 2, 3, 4, 5, 6} {
		require.Equal(s.t, epoch, got[id], "id %d: zero, partial zero and zero-year dates must arrive as the epoch", id)
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
		t.Logf("id %d: %s", id, v)
	}
	require.NoError(t, rs.Err())
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	return got
}

// A DATE or DATETIME partition key's MIN/MAX became the partition bounds through the value conversion, so a
// zero-part bound turned into the epoch: a partial-zero MAX put the end before the start, and a zero MIN dropped
// every row before 1970. Every row must arrive, including zero-part values inside the range and next to a
// partition boundary, and dates in year 0, which sort below every real date.
func (s MySQLPGSuite) Test_Zero_Date_Partition_Bounds() {
	s.zeroDatePartitions("zdp", shared.InternalVersion_Latest)
}

// Older mirrors (before InternalVersion_MySQLZeroDatePartsAsZero) partition the same way: only their values
// differ, so they read every row too.
func (s MySQLPGSuite) Test_Zero_Date_Partition_Bounds_Old_Version() {
	s.zeroDatePartitions("zdp_old", shared.InternalVersion_MySQLZeroDatePartsAsZero-1)
}

func (s MySQLPGSuite) zeroDatePartitions(prefix string, version uint32) {
	t := s.t
	for _, tc := range []struct {
		name, colType string
		values        []string
	}{
		{"date_year0_min", "DATE", []string{"0000-00-00", "0000-05-10", "1960-01-01", "2024-05-10", "2025-00-15",
			"2025-02-28", "2025-03-00", "2025-03-01", "2025-09-00"}},
		{"date_partial_min", "DATE", []string{"2024-03-00", "2024-03-01", "2024-05-10", "2025-03-00", "2025-03-01",
			"2025-12-00"}},
		{"datetime6", "DATETIME(6)", []string{"0000-00-00 00:00:00", "1960-01-01 00:00:00", "2025-02-28 23:59:59.999999",
			"2025-03-00 12:00:00", "2025-03-01 00:00:00", "2025-03-01 00:00:00.000001", "2025-04-00 00:00:00"}},
		{"datetime_year0_only", "DATETIME", []string{"0000-05-10 10:00:00", "0000-00-00 00:00:00"}},
	} {
		name := prefix + "_" + tc.name
		srcFull := AttachSchema(s, name)
		require.NoError(t, s.source.Exec(t.Context(), `SET SESSION sql_mode = ''`))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s (id INT AUTO_INCREMENT PRIMARY KEY, k %s)`, srcFull, tc.colType)))
		for _, v := range tc.values {
			require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (k) VALUES ('%s')`, srcFull, v)))
		}
		require.NoError(t, s.source.Exec(t.Context(), `ANALYZE TABLE `+srcFull))

		connectionGen := FlowConnectionGenerationConfig{
			FlowJobName:   AddSuffix(s, name),
			TableMappings: TableMappings(s, name, name),
			Destination:   s.Peer().Name,
		}
		flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
		flowConnConfig.TableMappings[0].PartitionKey = "k"
		flowConnConfig.DoInitialSnapshot = true
		flowConnConfig.SnapshotNumRowsPerPartition = 2
		flowConnConfig.Version = version
		temporal := NewTemporalClient(t)
		env := ExecutePeerflow(t, temporal, flowConnConfig)
		SetupCDCFlowStatusQuery(t, env, flowConnConfig)
		EnvWaitFor(t, env, 3*time.Minute, "initial load "+name, func() bool {
			return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, s.DestinationTable(name))) == int64(len(tc.values))
		})
		env.Cancel(t.Context())
		RequireEnvCanceled(t, env)
	}
}

// incrementalQRep starts a standalone QRep mirror on a DATETIME watermark k, cycling every 5 s
func (s MySQLPGSuite) incrementalQRep(name string) (WorkflowRun, string) {
	t := s.t
	srcFull := AttachSchema(s, name)
	qrepConfig := CreateQRepWorkflowConfig(t, AddSuffix(s, name), srcFull, s.DestinationTable(name),
		fmt.Sprintf("SELECT * FROM %s WHERE k BETWEEN {{.start}} AND {{.end}}", srcFull),
		s.Peer().Name, "", true, "", "")
	qrepConfig.SourceName = s.Source().GeneratePeer(t).Name
	qrepConfig.WatermarkColumn = "k"
	qrepConfig.InitialCopyOnly = false
	qrepConfig.WaitBetweenBatchesSeconds = 5
	qrepConfig.NumRowsPerPartition = 2
	return RunQRepFlowWorkflow(t, NewTemporalClient(t), qrepConfig), qrepConfig.FlowJobName
}

// Incremental QRep with a partial-zero minimum: the first cycle used to read the table as one partition that
// every later cycle took as already synced, so rows inserted after it never replicated. The minimum is floored to
// a real date instead, and later rows arrive.
func (s MySQLPGSuite) Test_Partial_Zero_Watermark_Incremental_QRep() {
	t := s.t
	name := "qrep_partial_zero_min"
	srcFull := AttachSchema(s, name)
	require.NoError(t, s.source.Exec(t.Context(), `SET SESSION sql_mode = ''`))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, k DATETIME NOT NULL)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, '2024-03-00 10:00:00'), (2, '2024-03-01 00:00:00'), (3, '2024-06-15 08:00:00')`, srcFull)))
	env, _ := s.incrementalQRep(name)
	dst := s.DestinationTable(name)
	// the destination table is created by the first cycle
	count := func() int64 {
		var n int64
		if err := s.dest.Conn().QueryRow(t.Context(), `SELECT count(*) FROM `+dst).Scan(&n); err != nil {
			return -1
		}
		return n
	}
	EnvWaitFor(t, env, 3*time.Minute, "first cycle", func() bool { return count() == 3 })
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(4, '2025-01-01 00:00:00'), (5, '2025-02-00 12:00:00')`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "rows inserted after the first cycle", func() bool { return count() == 5 })
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// Incremental QRep can't start a partition below year 1, where a zero year sorts: refused by name.
func (s MySQLPGSuite) Test_Zero_Year_Watermark_Refused_For_Incremental_QRep() {
	t := s.t
	name := "qrep_zero_year_min"
	srcFull := AttachSchema(s, name)
	require.NoError(t, s.source.Exec(t.Context(), `SET SESSION sql_mode = ''`))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, k DATETIME NOT NULL)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, '0000-05-10 10:00:00'), (2, '2024-03-01 00:00:00')`, srcFull)))
	env, flowName := s.incrementalQRep(name)
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	EnvWaitFor(t, env, 3*time.Minute, "refusal naming the watermark column", func() bool {
		count, err := GetLogCount(t.Context(), catalogPool, flowName, "error", "has dates in year 0")
		return err == nil && count > 0
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A zero date, or one with a zero part, arrives as the epoch, so in a key column distinct source rows would
// merge (Postgres: a unique violation in the snapshot, then merged rows in CDC; ClickHouse: silent dedup). The
// snapshot and CDC both refuse it, naming the column.
func (s MySQLPGSuite) Test_Zero_Date_Key_Refused() {
	t := s.t
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		snapshot bool
	}{{"zero_date_key_snapshot", true}, {"zero_date_key_cdc", false}} {
		srcFull := AttachSchema(s, tc.name)
		require.NoError(t, s.source.Exec(t.Context(), `SET SESSION sql_mode = ''`))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
			`CREATE TABLE %s (k DATE PRIMARY KEY, v INT)`, srcFull)))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
			`INSERT INTO %s VALUES ('2024-05-10', 1)`, srcFull)))
		if tc.snapshot {
			require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s VALUES ('2025-03-00', 2)`, srcFull)))
		}
		connectionGen := FlowConnectionGenerationConfig{
			FlowJobName:   AddSuffix(s, tc.name),
			TableMappings: TableMappings(s, tc.name, tc.name),
			Destination:   s.Peer().Name,
		}
		flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
		flowConnConfig.DoInitialSnapshot = true
		env := ExecutePeerflow(t, NewTemporalClient(t), flowConnConfig)
		if !tc.snapshot { // a failing snapshot never reaches RUNNING
			SetupCDCFlowStatusQuery(t, env, flowConnConfig)
			EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
				return s.destInt(`SELECT count(*) FROM `+s.DestinationTable(tc.name)) == 1
			})
			EnvNoError(t, env, s.source.Exec(t.Context(), `SET SESSION sql_mode = ''`))
			EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
				`INSERT INTO %s VALUES ('2025-03-00', 2)`, srcFull)))
		}
		EnvWaitFor(t, env, 3*time.Minute, "error naming the key column "+tc.name, func() bool {
			count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error",
				"primary key column k of")
			return err == nil && count > 0
		})
		require.Zero(t, s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE k = '1970-01-01'`,
			s.DestinationTable(tc.name))), "a zero-date key must not arrive as the epoch")
		env.Cancel(t.Context())
		RequireEnvCanceled(t, env)
	}
}
