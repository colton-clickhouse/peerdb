package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// precreatedColumns are the columns of precreatedMirror: a type PeerDB maps to text on its own, with the target column
// pre-created as the real Postgres type, and the value (a literal on both sides) and its text form on the target.
// Mirrors older than minVersion leave the column out.
var precreatedColumns = []struct {
	name, srcType, dstType, value, want string
	minVersion                          uint32
}{
	{"c_enum", "{mood}", "{mood}", "'sad'", "sad", 0},
	{"c_enum_arr", "{mood}[]", "{mood}[]", "ARRAY['sad', 'ok']::{mood}[]", "{sad,ok}", 0},
	// a NULL element, and labels that need quoting in array text (one spelled NULL); before ArrayNullElements the NULL
	// arrived as '', which no enum takes, so an older mirror's snapshot would never finish
	{"c_enum_arr_q", "{mood}[]", "{mood}[]", "ARRAY['in progress', NULL, 'a,b', 'NULL']::{mood}[]",
		`{"in progress",NULL,"a,b","NULL"}`, shared.InternalVersion_ArrayNullElements},
	{"c_bit", "BIT(8)", "bit(8)", "B'10101010'", "10101010", 0},
	{"c_varbit", "VARBIT", "bit varying", "B'101'", "101", 0}, // the name format_type gives
}

// A target pre-created with Postgres enum and bit types must replicate through CDC as it does through the snapshot (before,
// normalize cast every value to PeerDB's own type, text, and the first CDC change failed with 42804 for the whole mirror,
// which kept reporting STATUS_RUNNING). Rows: id 2 from the snapshot, id 1 snapshot then updated, id 101 a CDC insert.
func (s CRDBPGSuite) Test_Precreated_Enum_And_Bit_Targets() {
	t := s.t
	srcTable := "precreated_types"
	dstFull := s.DestinationTable(srcTable)
	env, _ := s.precreatedMirror(srcTable, shared.InternalVersion_Latest)
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 101 OR (id = 1 AND n = 1)`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	var bad []string
	for _, id := range []int{1, 2, 101} {
		for _, c := range precreatedColumns {
			if got := s.destText(fmt.Sprintf(`SELECT %s::text FROM %s WHERE id = %d`, c.name, dstFull, id)); got != c.want {
				bad = append(bad, fmt.Sprintf("%d %s: %s, want %s", id, c.name, got, c.want))
			}
		}
	}
	if len(bad) > 0 {
		t.Errorf("%d values differ:\n  %s", len(bad), strings.Join(bad, "\n  "))
	}
	// the target keeps the types it was created with
	for _, c := range precreatedColumns {
		require.Equal(t, strings.ReplaceAll(c.dstType, "{mood}", s.DestinationTable(srcTable+"_mood")),
			s.destText(fmt.Sprintf(`SELECT format_type(atttypid, atttypmod) FROM pg_attribute
				WHERE attrelid = '%s'::regclass AND attname = '%s'`, dstFull, c.name)), c.name)
	}
}

// Mirrors created before InternalVersion_PgNormalizeCastToTargetTypes still cast to text and fail the same way, but the
// error is user-actionable (NOTIFY_INVALID_DESTINATION_TABLE_DEFINITION), so the mirror goes STATUS_STALLED instead of
// retrying forever as RUNNING.
func (s CRDBPGSuite) Test_Precreated_Enum_Target_Old_Version_Stalls() {
	t := s.t
	client, err := NewApiClient()
	require.NoError(t, err)
	SetDynamicSetting(t, "PEERDB_STALLED_THRESHOLD_MINUTES", "1")

	env, flowConnConfig := s.precreatedMirror("precreated_types_old", shared.InternalVersion_PgNormalizeCastToTargetTypes-1)
	EnvWaitFor(t, env, 6*time.Minute, "STALLED while normalize fails", func() bool {
		res, err := client.MirrorStatus(t.Context(), &protos.MirrorStatusRequest{FlowJobName: flowConnConfig.FlowJobName})
		if err != nil {
			t.Log("mirror status:", err)
			return false
		}
		return res.CurrentFlowState == protos.FlowStatus_STATUS_STALLED
	})
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "warn",
		"Mirror is STALLED%NOTIFY_INVALID_DESTINATION_TABLE_DEFINITION")
	EnvNoError(t, env, err)
	require.Positive(t, count, "the mirror's log says why it is STALLED")
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// A label added to the source enum mid-stream is missing from the pre-created target enum (the customer's case for
// enums: the target type is created by hand, so it doesn't follow the source). The insert fails in normalize with
// NOTIFY_INVALID_ENUM_VALUE, so the mirror goes STATUS_STALLED; adding the label on the target lets the same batch
// through, and the mirror is RUNNING again without losing the row.
func (s CRDBPGSuite) Test_Precreated_Enum_New_Source_Label() {
	t := s.t
	srcTable := "precreated_label"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	client, err := NewApiClient()
	require.NoError(t, err)
	SetDynamicSetting(t, "PEERDB_STALLED_THRESHOLD_MINUTES", "1")

	env, flowConnConfig := s.precreatedMirror(srcTable, shared.InternalVersion_Latest)
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 101 OR (id = 1 AND n = 1)`, dstFull)) == 2
	})
	status := func() protos.FlowStatus {
		res, err := client.MirrorStatus(t.Context(), &protos.MirrorStatusRequest{FlowJobName: flowConnConfig.FlowJobName})
		if err != nil {
			t.Log("mirror status:", err)
			return protos.FlowStatus_STATUS_UNKNOWN
		}
		return res.CurrentFlowState
	}

	EnvNoError(t, env, s.source.Exec(t.Context(),
		fmt.Sprintf(`ALTER TYPE %s ADD VALUE 'meh'`, AttachSchema(s, srcTable+"_mood"))))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, n, c_enum) VALUES (102, 0, 'meh')`, srcFull)))
	EnvWaitFor(t, env, 6*time.Minute, "STALLED while the target enum lacks the label", func() bool {
		return status() == protos.FlowStatus_STATUS_STALLED
	})
	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "warn",
		"Mirror is STALLED%NOTIFY_INVALID_ENUM_VALUE")
	EnvNoError(t, env, err)
	require.Positive(t, count, "the mirror's log says why it is STALLED")

	_, err = s.dest.Conn().Exec(t.Context(),
		fmt.Sprintf(`ALTER TYPE %s ADD VALUE 'meh'`, s.DestinationTable(srcTable+"_mood")))
	EnvNoError(t, env, err)
	EnvWaitFor(t, env, 8*time.Minute, "RUNNING again with the row", func() bool {
		return status() == protos.FlowStatus_STATUS_RUNNING &&
			s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 102 AND c_enum = 'meh'`, dstFull)) == 1
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

// precreatedMirror creates srcTable with precreatedColumns, pre-creates its target with the Postgres types, starts a
// mirror at the given internal version, waits for the snapshot (ids 1 and 2) and then inserts id 101 and updates id 1.
func (s CRDBPGSuite) precreatedMirror(srcTable string, version uint32) (WorkflowRun, *protos.FlowConnectionConfigs) {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	srcMood, dstMood := AttachSchema(s, srcTable+"_mood"), s.DestinationTable(srcTable+"_mood")
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %s AS ENUM ('ok', 'sad', 'in progress', 'a,b', 'NULL')`, srcMood)))
	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %s AS ENUM ('ok', 'sad', 'in progress', 'a,b', 'NULL')`, dstMood))
	require.NoError(t, err)

	srcDefs := make([]string, 0, len(precreatedColumns))
	dstDefs := make([]string, 0, len(precreatedColumns))
	vals := make([]string, 0, len(precreatedColumns))
	for _, c := range precreatedColumns {
		if version < c.minVersion {
			continue
		}
		srcDefs = append(srcDefs, c.name+" "+strings.ReplaceAll(c.srcType, "{mood}", srcMood))
		dstDefs = append(dstDefs, c.name+" "+strings.ReplaceAll(c.dstType, "{mood}", dstMood))
		vals = append(vals, strings.ReplaceAll(c.value, "{mood}", srcMood))
	}
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, n INT8, %s)`,
		srcFull, strings.Join(srcDefs, ", "))))
	// as PeerDB would create it, with the mirror's synced-at column
	_, err = s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, n bigint, %s,
		"_PEERDB_SYNCED_AT" timestamp DEFAULT CURRENT_TIMESTAMP)`, dstFull, strings.Join(dstDefs, ", ")))
	require.NoError(t, err)
	row := func(id int) string { return fmt.Sprintf("(%d, 0, %s)", id, strings.Join(vals, ", ")) }
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s, %s`, srcFull, row(1), row(2))))

	flowConnConfig := s.startMirror(srcTable)
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE c_enum = 'sad' AND c_bit = B'10101010'`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES %s`, srcFull, row(101))))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET n = 1 WHERE id = 1`, srcFull)))
	return env, flowConnConfig
}
