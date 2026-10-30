package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// arrayIntegrityRows are a snapshot row (1), a CDC insert (2) and a CDC update of row 1, with +-infinity and NULL
// elements in date, timestamp and timestamptz arrays and NULL elements in an int array.
const arrayIntegrityValues = `ARRAY['2024-01-02','infinity','-infinity',NULL]::date[],
	ARRAY['2024-01-02 03:04:05','infinity','-infinity',NULL]::timestamp[],
	ARRAY['2024-01-02 03:04:05+00','infinity','-infinity',NULL]::timestamptz[],
	ARRAY[1,NULL,3]::int8[],
	ARRAY[1.5,NULL,'NaN','Infinity','-Infinity']::numeric[]`

func (s PeerFlowE2ETestSuitePG) arrayIntegrityMirror(name string, version uint32) (string, string) {
	t := s.t
	srcTableName := s.attachSchemaSuffix(name)
	dstTableName := s.attachSchemaSuffix(name + "_dst")
	_, err := s.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v INT,
		d date[], ts timestamp[], tstz timestamptz[], i int8[], n numeric[])`, srcTableName))
	require.NoError(t, err)
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 0, %s)`, srcTableName, arrayIntegrityValues))
	require.NoError(t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(name),
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.SoftDeleteColName = ""
	flowConnConfig.SyncedAtColName = ""
	flowConnConfig.System = protos.TypeSystem_Q
	flowConnConfig.DoInitialSnapshot = true
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		var n int
		return s.Conn().QueryRow(t.Context(), "SELECT count(*) FROM "+dstTableName).Scan(&n) == nil && n == 1
	})
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 0, %s)`, srcTableName, arrayIntegrityValues))
	EnvNoError(t, env, err)
	_, err = s.Conn().Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 1 WHERE id = 1`, srcTableName))
	EnvNoError(t, env, err)
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		var n int
		return s.Conn().QueryRow(t.Context(), fmt.Sprintf(
			"SELECT count(*) FROM %s WHERE id = 2 OR (id = 1 AND v = 1)", dstTableName)).Scan(&n) == nil && n == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	return srcTableName, dstTableName
}

// A Postgres source's +-infinity and NULL array elements reach a Postgres destination as infinity and NULL on both
// paths, instead of 0001-01-01 / 0.
func (s PeerFlowE2ETestSuitePG) Test_Array_Infinity_And_Null_Elements_PG() {
	src, dst := s.arrayIntegrityMirror("array_integrity", shared.InternalVersion_Latest)
	require.NoError(s.t, s.comparePGTables(src, dst, "id,d::text,ts::text,tstz::text,i::text,n::text"))
}

// Mirrors created before InternalVersion_ArrayConversionIntegrity keep writing the zero values.
func (s PeerFlowE2ETestSuitePG) Test_Array_Infinity_And_Null_Elements_PG_Old_Version() {
	_, dst := s.arrayIntegrityMirror("array_integrity_old", shared.InternalVersion_ArrayConversionIntegrity-1)
	for _, id := range []int{1, 2} {
		var d, ts, tstz, i, n string
		require.NoError(s.t, s.Conn().QueryRow(s.t.Context(), fmt.Sprintf(
			"SELECT d::text, ts::text, (SELECT array_agg(e AT TIME ZONE 'UTC' ORDER BY o) FROM unnest(tstz) WITH ORDINALITY u(e, o))::text,"+
				" i::text, n::text FROM %s WHERE id = %d", dst, id),
		).Scan(&d, &ts, &tstz, &i, &n))
		require.Equal(s.t, "{2024-01-02,0001-01-01,0001-01-01,0001-01-01}", d, "id %d", id)
		require.Equal(s.t, `{"2024-01-02 03:04:05","0001-01-01 00:00:00","0001-01-01 00:00:00","0001-01-01 00:00:00"}`, ts,
			"id %d", id)
		require.Equal(s.t, "{1,0,3}", i, "id %d", id)
		require.Equal(s.t, "{1.5,0,0,0,0}", n, "id %d: NaN and infinity elements stay 0 on older mirrors", id)
		require.Equal(s.t, ts, tstz, "id %d: timestamptz like timestamp (in UTC)", id)
	}
}

// ClickHouse can't store infinity: +-infinity date array elements become its DefaultTime (the Unix epoch) on both
// paths, so the snapshot row and the CDC row agree.
func (s ClickHouseSuite) Test_Array_Infinity_Elements_CH() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("infinity dates are a Postgres source case")
	}
	const srcTable = "test_array_infinity_ch"
	srcSchemaTable := s.attachSchemaSuffix(srcTable)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, d date[])`, srcSchemaTable)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, ARRAY['2024-01-02','infinity','-infinity',NULL]::date[])`, srcSchemaTable)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTable),
		TableNameMapping: map[string]string{srcSchemaTable: srcTable},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvNoError(s.t, env, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, ARRAY['2024-01-02','infinity','-infinity',NULL]::date[])`, srcSchemaTable)))

	var got []string
	EnvWaitFor(s.t, env, 3*time.Minute, "snapshot and cdc rows", func() bool {
		rows, err := s.GetRows(srcTable, "id, toString(d) AS d")
		if err != nil || len(rows.Records) != 2 {
			return false
		}
		got = got[:0]
		for _, r := range rows.Records {
			got = append(got, fmt.Sprint(r[1].Value()))
		}
		return true
	})
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
	for i, d := range got {
		require.Equal(s.t, "['2024-01-02','1970-01-01','1970-01-01','1970-01-01']", strings.ReplaceAll(d, " ", ""),
			"row %d: infinity and NULL elements are ClickHouse's DefaultTime", i+1)
	}
}
