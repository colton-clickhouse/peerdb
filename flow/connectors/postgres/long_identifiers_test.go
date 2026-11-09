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
