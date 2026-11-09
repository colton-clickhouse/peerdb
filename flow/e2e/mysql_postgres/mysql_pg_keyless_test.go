//go:build tilt

package mysql_postgres

import (
	"fmt"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Keyless tables to a Postgres destination. Normalize matches rows by primary key; with none, the MERGE
// renders `PARTITION BY ORDER BY ...` and fails, and because normalize runs every table of a batch together,
// CDC stops for the whole mirror (still reported Running, with the error logged against another table). Until
// keyless CDC is supported, mirror validation must refuse such a table and name it. A snapshot-only mirror
// never normalizes, so it may keep keyless tables.
func (s MySQLPGSuite) Test_Keyless_Table_Refused_At_Validation() {
	t := s.t
	keyless, keyed := "keyless_nopk", "keyless_keyed"
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT, v VARCHAR(20))`, e2e.AttachSchema(s, keyless))))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, e2e.AttachSchema(s, keyed))))

	client, err := e2e.NewApiClient()
	require.NoError(t, err)
	validate := func(snapshotOnly bool, tables ...string) error {
		t.Helper()
		pairs := make([]string, 0, 2*len(tables))
		for _, table := range tables {
			pairs = append(pairs, table, table)
		}
		connectionGen := e2e.FlowConnectionGenerationConfig{
			FlowJobName:   e2e.AddSuffix(s, "keyless_validation"),
			TableMappings: e2e.TableMappings(s, pairs...),
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
	require.Error(t, err, "keyless table in a CDC mirror to Postgres must be refused at validation")
	require.Contains(t, err.Error(), keyless, "the refusal must name the keyless table")
	require.NotContains(t, err.Error(), keyed, "the refusal must not name keyed tables")
}

// Excluding a primary key column: normalize would match rows by the rest of the key, so rows differing only in
// the excluded column would overwrite each other. MySQL reports the key without excluded columns, so validation
// must compare the exclusions with the source table's own key and name the column.
func (s MySQLPGSuite) Test_Excluded_Key_Column_Refused_At_Validation() {
	t := s.t
	composite, single := "exclkey_composite", "exclkey_single"
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (a INT, b_part INT, v VARCHAR(20), PRIMARY KEY (a, b_part))`, e2e.AttachSchema(s, composite))))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(20))`, e2e.AttachSchema(s, single))))

	client, err := e2e.NewApiClient()
	require.NoError(t, err)
	validate := func(table string, exclude ...string) error {
		t.Helper()
		connectionGen := e2e.FlowConnectionGenerationConfig{
			FlowJobName:   e2e.AddSuffix(s, "exclkey_validation"),
			TableMappings: e2e.TableMappings(s, table, table),
			Destination:   s.Peer().Name,
		}
		flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
		flowConnConfig.DoInitialSnapshot = true
		flowConnConfig.TableMappings[0].Exclude = exclude
		_, err := client.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
		return err
	}

	// control: excluding a non-key column is fine
	require.NoError(t, validate(composite, "v"), "non-key column excluded")
	err = validate(composite, "b_part")
	require.Error(t, err, "an excluded key column in a CDC mirror to Postgres must be refused at validation")
	require.Contains(t, err.Error(), "primary key columns of")
	require.Contains(t, err.Error(), "b_part", "the refusal must name the excluded key column")
	// the whole key excluded: refused as an excluded key, not as a table without a primary key
	err = validate(single, "id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key columns of")
	require.NotContains(t, err.Error(), "without a primary key")
}
