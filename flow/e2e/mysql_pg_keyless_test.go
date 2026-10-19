package e2e

import (
	"fmt"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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
