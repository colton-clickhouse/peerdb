package connpostgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestLongIdentifiersError(t *testing.T) {
	long := strings.Repeat("c", 64)
	schemas := map[string]*protos.TableSchema{
		"src.t": {Columns: []*protos.FieldDescription{{Name: "id"}, {Name: long}}},
	}
	mapping := func(dst string, exclude ...string) []*protos.TableMapping {
		return []*protos.TableMapping{{SourceTableIdentifier: "src.t", DestinationTableIdentifier: dst, Exclude: exclude}}
	}

	err := longIdentifiersError(mapping("dst.t"), schemas)
	require.ErrorContains(t, err, "column dst.t."+long)
	// excluded columns aren't created
	require.NoError(t, longIdentifiersError(mapping("dst.t", long), schemas))
	// 63 bytes is the limit, counted in bytes: 32 two-byte characters are 64 bytes
	require.NoError(t, longIdentifiersError(mapping("dst."+strings.Repeat("t", 63), long), schemas))
	require.ErrorContains(t, longIdentifiersError(mapping("dst."+strings.Repeat("é", 32), long), schemas),
		"table dst."+strings.Repeat("é", 32))
}

func TestLongPeerDBNames(t *testing.T) {
	ok := strings.Repeat("m", maxIdentifierBytes-len(getRawTableIdentifier("")))
	require.NoError(t, longPeerDBNamesError(&protos.FlowConnectionConfigsCore{FlowJobName: ok}))
	err := longPeerDBNamesError(&protos.FlowConnectionConfigsCore{FlowJobName: ok + "x",
		SoftDeleteColName: strings.Repeat("d", 64), SyncedAtColName: "_PEERDB_SYNCED_AT"})
	require.ErrorContains(t, err, "mirror name "+ok+"x")
	require.ErrorContains(t, err, "soft delete column")
	require.NotContains(t, err.Error(), "synced-at column")

	// index names of long raw tables stay unique within 63 bytes
	a, b := getRawTableIdentifier(ok), getRawTableIdentifier(ok[:len(ok)-1]+"n")
	for _, suffix := range []string{"_batchid_idx", "_dst_table_idx"} {
		require.LessOrEqual(t, len(rawTableIndexName(a, suffix)), maxIdentifierBytes)
		require.NotEqual(t, rawTableIndexName(a, suffix), rawTableIndexName(b, suffix))
		require.True(t, strings.HasSuffix(rawTableIndexName(a, suffix), suffix))
	}
	require.Equal(t, "_peerdb_raw_m_batchid_idx", rawTableIndexName(getRawTableIdentifier("m"), "_batchid_idx"))
}
