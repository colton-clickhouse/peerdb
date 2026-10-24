package conncockroachdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDestinationKeyColumns(t *testing.T) {
	// plain key
	key, err := destinationKeyColumns([]sourceKeyColumn{{name: "id"}, {name: "region"}}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "region"}, key)

	// hash-sharded key (#12): the shard column is computed from the other key columns, so it is dropped
	shard := sourceKeyColumn{
		name: "crdb_internal_id_ts_shard_16", virtual: true,
		expression: "mod(fnv32(md5(crdb_internal.datums_to_bytes(id, ts))), 16)",
	}
	all := []string{"ts", "id", "v", "crdb_internal_id_ts_shard_16"}
	key, err = destinationKeyColumns([]sourceKeyColumn{shard, {name: "ts"}, {name: "id"}}, all)
	require.NoError(t, err)
	require.Equal(t, []string{"ts", "id"}, key)

	// a user virtual key column computed from key columns only: dropped too
	key, err = destinationKeyColumns([]sourceKeyColumn{{name: "b", virtual: true, expression: "a % 10"}, {name: "a"}},
		[]string{"a", "b", "c"})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, key)

	// computed from a non-key column: dropping it could merge rows, so refuse
	_, err = destinationKeyColumns([]sourceKeyColumn{{name: "b", virtual: true, expression: "c % 10"}, {name: "a"}},
		[]string{"a", "b", "c"})
	require.ErrorContains(t, err, "depends on c, which is not in the key")

	// only virtual key columns
	_, err = destinationKeyColumns([]sourceKeyColumn{{name: "b", virtual: true, expression: "lower(v)"}}, []string{"b"})
	require.ErrorContains(t, err, "only virtual computed columns")

	// stored computed key columns are replicated, so they stay in the key
	key, err = destinationKeyColumns([]sourceKeyColumn{{name: "s", expression: "c % 10"}, {name: "a"}},
		[]string{"a", "s", "c"})
	require.NoError(t, err)
	require.Equal(t, []string{"s", "a"}, key)
}

func TestReferencesColumn(t *testing.T) {
	require.True(t, referencesColumn("a % 10", "a"))
	require.True(t, referencesColumn("datums_to_bytes(id, ts)", "ts"))
	require.True(t, referencesColumn(`lower("My Col")`, "My Col"))
	require.True(t, referencesColumn(`lower("say ""hi""")`, `say "hi"`))
	require.False(t, referencesColumn("abc % 10", "a"))
	require.False(t, referencesColumn("ts_2 + 1", "ts"))
}
