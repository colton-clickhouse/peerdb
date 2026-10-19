package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestTablesWithoutPrimaryKey(t *testing.T) {
	schemas := map[string]*protos.TableSchema{
		"s.keyed":     {PrimaryKeyColumns: []string{"id"}},
		"s.composite": {PrimaryKeyColumns: []string{"a", "b"}},
		// a Postgres source reports every column as the key of a REPLICA IDENTITY FULL table
		"s.replident_full": {PrimaryKeyColumns: []string{"a", "b", "c"}, IsReplicaIdentityFull: true},
		"s.keyless":        {},
		"s.keyless2":       {PrimaryKeyColumns: []string{}},
	}
	require.Equal(t, []string{"s.keyless", "s.keyless2"}, tablesWithoutPrimaryKey(
		[]string{"s.keyed", "s.keyless", "s.composite", "s.replident_full", "s.keyless2"}, schemas))
	require.Empty(t, tablesWithoutPrimaryKey([]string{"s.keyed", "s.composite", "s.replident_full"}, schemas))
	// a table without a schema isn't reported here (validation reports it as not found)
	require.Empty(t, tablesWithoutPrimaryKey([]string{"s.unknown"}, schemas))
}
