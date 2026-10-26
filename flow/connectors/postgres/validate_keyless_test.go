package connpostgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestKeylessTablesError(t *testing.T) {
	schemas := map[string]*protos.TableSchema{
		"s.keyed":     {PrimaryKeyColumns: []string{"id"}},
		"s.ri_full":   {PrimaryKeyColumns: []string{"a", "b"}, IsReplicaIdentityFull: true}, // a PG source keys it on all columns
		"s.ri_index":  {PrimaryKeyColumns: []string{"u"}},                                   // REPLICA IDENTITY USING INDEX
		"s.keyless_b": {},
		"s.keyless_a": {},
	}
	require.NoError(t, keylessTablesError([]string{"s.keyed", "s.ri_full", "s.ri_index"}, schemas, false))
	// a table without a schema isn't named (fails open)
	require.NoError(t, keylessTablesError([]string{"s.unknown"}, schemas, false))

	err := keylessTablesError([]string{"s.keyless_b", "s.keyed", "s.keyless_a"}, schemas, false)
	var keyless *exceptions.KeylessTablesError
	require.ErrorAs(t, err, &keyless)
	require.Equal(t, []string{"s.keyless_b", "s.keyless_a"}, keyless.Tables, "in the mirror's order")
	require.Contains(t, err.Error(), "add a primary key on the source or remove them from the mirror")

	// a running mirror keeps its schema: adding a key needs a recreate
	err = keylessTablesError([]string{"s.keyless_a"}, schemas, true)
	require.Contains(t, err.Error(), "add a primary key on the source and recreate the mirror")
}

// Excluding a key column would match rows by the rest of the key, so rows differing only in it would overwrite
// each other: refused at validation, before anything else.
func TestValidateMirrorDestinationRefusesExcludedKeyColumns(t *testing.T) {
	c := &PostgresConnector{}
	cfg := &protos.FlowConnectionConfigsCore{TableMappings: []*protos.TableMapping{
		{SourceTableIdentifier: "s.t", DestinationTableIdentifier: "d.t", Exclude: []string{"b", "note"}},
	}}
	schemas := map[string]*protos.TableSchema{"s.t": {PrimaryKeyColumns: []string{"a", "b"}}}
	err := c.ValidateMirrorDestination(t.Context(), cfg, schemas)
	var excluded *exceptions.ExcludedKeyColumnsError
	require.ErrorAs(t, err, &excluded)
	require.Equal(t, []string{"b"}, excluded.Columns)
}
