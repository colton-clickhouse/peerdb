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
	require.NoError(t, keylessTablesError([]string{"s.keyed", "s.ri_full", "s.ri_index"}, schemas))
	// a table without a schema isn't named (fails open)
	require.NoError(t, keylessTablesError([]string{"s.unknown"}, schemas))

	err := keylessTablesError([]string{"s.keyless_b", "s.keyed", "s.keyless_a"}, schemas)
	var keyless *exceptions.KeylessTablesError
	require.ErrorAs(t, err, &keyless)
	require.Equal(t, []string{"s.keyless_b", "s.keyless_a"}, keyless.Tables, "in the mirror's order")
	require.Contains(t, err.Error(), "add a primary key on the source or remove them from the mirror")
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
	require.False(t, excluded.WholeKey)
	require.Contains(t, err.Error(), "matched by the rest of the key")

	// the only key column: nothing left to match on, and not reported as a table without a key
	cfg.TableMappings[0].Exclude = []string{"id"}
	err = c.ValidateMirrorDestination(t.Context(), cfg, map[string]*protos.TableSchema{"s.t": {PrimaryKeyColumns: []string{"id"}}})
	require.ErrorAs(t, err, &excluded)
	require.True(t, excluded.WholeKey)
	require.Contains(t, err.Error(), "the whole key")
}
