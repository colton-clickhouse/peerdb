package e2e

import (
	"fmt"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// MySQL identifiers can be 64 characters, Postgres truncates them to 63 bytes: two columns that share their
// first 63 bytes collide on the target ("column ... specified more than once"), and as table setup is one
// batch retried forever, that blocks setup and the snapshot for the whole mirror. Validation must refuse it.
func (s MySQLPGSuite) Test_Long_Identifiers_Refused_At_Validation() {
	t := s.t
	long, short := "longid_collide", "longid_ok"
	colA := "col_this_column_name_is_exactly_sixty_four_chars_long_variant_xa"
	colB := "col_this_column_name_is_exactly_sixty_four_chars_long_variant_xb"
	require.Len(t, colA, 64)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, `%s` VARCHAR(10), `%s` VARCHAR(10))", AttachSchema(s, long), colA, colB)))
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(10))`, AttachSchema(s, short))))

	client, err := NewApiClient()
	require.NoError(t, err)
	validate := func(tables ...string) error {
		t.Helper()
		pairs := make([]string, 0, 2*len(tables))
		for _, table := range tables {
			pairs = append(pairs, table, table)
		}
		connectionGen := FlowConnectionGenerationConfig{
			FlowJobName:   AddSuffix(s, "long_ident_validation"),
			TableMappings: TableMappings(s, pairs...),
			Destination:   s.Peer().Name,
		}
		flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
		flowConnConfig.DoInitialSnapshot = true
		_, err := client.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
		return err
	}

	require.NoError(t, validate(short), "control")
	err = validate(long, short)
	require.Error(t, err, "columns colliding at 63 bytes must be refused at validation")
	require.Contains(t, err.Error(), long, "the refusal must name the table")
	require.Contains(t, err.Error(), colA, "the refusal must name the column")
	require.NotContains(t, err.Error(), short, "the refusal must not name other tables")
}
