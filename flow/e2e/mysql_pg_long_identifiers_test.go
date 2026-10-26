package e2e

import (
	"context"
	"fmt"
	"strings"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/activities"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
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

	// a mirror name whose raw table _peerdb_raw_<name> Postgres would truncate
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   strings.Repeat("m", 63-len("_peerdb_raw_")+1),
		TableMappings: TableMappings(s, short, short),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	_, err = client.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.ErrorContains(t, err, "mirror name "+flowConnConfig.FlowJobName)
}

// Mirrors created before validation refused long names: two mirrors into one database whose raw table names
// Postgres truncates to the same name share the raw table. The sync refuses that, naming both, even when they use
// different peers for the same database.
func (s MySQLPGSuite) Test_Shared_Raw_Table_Refused() {
	t := s.t
	ctx := t.Context()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	peer := s.Peer()
	alias := proto.Clone(peer).(*protos.Peer)
	alias.Name = peer.Name + "_alias"
	CreatePeer(t, alias)
	prefix := strings.Repeat("m", connpostgres.MaxIdentifierBytes-len(connpostgres.RawTableIdentifier("")))
	a, b := prefix+"_a_"+s.Suffix(), prefix+"_b_"+s.Suffix()
	for _, f := range []struct{ name, peer string }{{a, peer.Name}, {b, alias.Name}} {
		_, err := pool.Exec(ctx, `INSERT INTO flows(name, source_peer, destination_peer, workflow_id, status)
			SELECT $1::text, p.id, p.id, $1::text, 1 FROM peers p WHERE p.name = $2`, f.name, f.peer)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM flows WHERE name = ANY($1)`, []string{a, b})
		_, _ = pool.Exec(context.Background(), `DELETE FROM peers WHERE name = $1`, alias.Name)
	})
	var shared *exceptions.SharedRawTableError
	require.ErrorAs(t, activities.CheckSharedRawTable(ctx, pool, a, peer.Name), &shared)
	require.Equal(t, b, shared.OtherMirror)
	// a mirror whose raw table name fits isn't affected
	require.NoError(t, activities.CheckSharedRawTable(ctx, pool, "short_"+s.Suffix(), peer.Name))
}
