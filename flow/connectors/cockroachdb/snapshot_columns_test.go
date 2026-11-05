package conncockroachdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// The snapshot's columns must be the ones the mirror stored at setup: a column dropped, added or retyped in between
// fails the snapshot instead of leaving the target table out of step with it.
func TestSameSnapshotColumns(t *testing.T) {
	col := func(name string, kind types.QValueKind) *protos.FieldDescription {
		return &protos.FieldDescription{Name: name, Type: string(kind)}
	}
	stored := []*protos.FieldDescription{col("id", types.QValueKindInt64), col("b", types.QValueKindInt64)}

	reordered := []*protos.FieldDescription{col("b", types.QValueKindInt64), col("id", types.QValueKindInt64)}
	_, _, ok := sameSnapshotColumns(reordered, stored, nil)
	require.True(t, ok, "order doesn't matter")
	// an excluded column is in the snapshot's schema but not in the stored one
	_, _, ok = sameSnapshotColumns(append([]*protos.FieldDescription{col("x", types.QValueKindString)}, stored...),
		stored, []string{"x"})
	require.True(t, ok)

	for name, atSnapshot := range map[string][]*protos.FieldDescription{
		"dropped": {col("id", types.QValueKindInt64)},
		"added":   append([]*protos.FieldDescription{col("c", types.QValueKindString)}, stored...),
		"retyped": {col("id", types.QValueKindInt64), col("b", types.QValueKindString)},
	} {
		got, want, ok := sameSnapshotColumns(atSnapshot, stored, nil)
		require.False(t, ok, name)
		require.NotEqual(t, got, want, name)
	}
}
