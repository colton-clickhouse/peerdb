package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// Keyless tables to a Postgres destination (MySQL -> Managed PG Pass 1 finding #18). Normalize matches rows
// by primary key; with none, the MERGE renders `PARTITION BY ORDER BY ...`, fails, and because normalize
// runs every table of a batch together, CDC stops for the whole mirror (still reported Running, with the
// error logged against another table). Until keyless CDC is supported, mirror validation must refuse such
// a table and name it. A snapshot-only mirror never normalizes, so it may keep keyless tables.
func (s MySQLPGSuite) Test_Keyless_Table_Refused_At_Validation() {
	t := s.t
	keyless, keyed := "keyless_nopk", "keyless_keyed"
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT, v VARCHAR(20))`, AttachSchema(s, keyless))))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, AttachSchema(s, keyed))))

	client, err := NewApiClient()
	require.NoError(t, err)
	validate := func(snapshotOnly bool, tables ...string) error {
		t.Helper()
		pairs := make([]string, 0, 2*len(tables))
		for _, table := range tables {
			pairs = append(pairs, table, table)
		}
		connectionGen := FlowConnectionGenerationConfig{
			FlowJobName:   AddSuffix(s, "keyless_validation"),
			TableMappings: TableMappings(s, pairs...),
			Destination:   s.Peer().Name,
		}
		flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
		flowConnConfig.DoInitialSnapshot = true
		flowConnConfig.InitialSnapshotOnly = snapshotOnly
		_, err := client.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
		return err
	}

	// control: the validation call works from here
	require.NoError(t, validate(false, keyed), "keyed table, CDC")
	// snapshot only: no normalize, keyless is fine
	require.NoError(t, validate(true, keyless, keyed), "keyless table, snapshot only")
	// CDC: refused, naming the table
	err = validate(false, keyless, keyed)
	require.Error(t, err, "keyless table in a CDC mirror to Postgres must be refused at validation (#18)")
	require.Contains(t, err.Error(), keyless, "the refusal must name the keyless table")
	require.NotContains(t, err.Error(), keyed, "the refusal must not name keyed tables")
}

// A keyless table that got into a mirror anyway (validation skipped, or a mirror from before the check):
// normalize must fail with an error naming that table, not a malformed MERGE reported under another table.
func (s MySQLPGSuite) Test_Keyless_Table_Normalize_Error_Names_Table() {
	t := s.t
	keyless, keyed := "keyless_cdc_nopk", "keyless_cdc_keyed"
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT, v VARCHAR(20))`, AttachSchema(s, keyless))))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, AttachSchema(s, keyed))))

	// started directly, so mirror validation doesn't run
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, keyless),
		TableMappings: TableMappings(s, keyless, keyless, keyed, keyed),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, AttachSchema(s, keyless))))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, AttachSchema(s, keyed))))

	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
	require.NoError(t, err)
	// GetLogCount pastes the pattern into SQL quotes, so no apostrophes
	pattern := "replicated with CDC to Postgres: " + s.DestinationTable(keyless)
	EnvWaitFor(t, env, 3*time.Minute, "normalize error naming the keyless table (#18)", func() bool {
		count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error", pattern)
		if err != nil {
			t.Log("error querying flow_errors:", err)
			return false
		}
		return count > 0
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
