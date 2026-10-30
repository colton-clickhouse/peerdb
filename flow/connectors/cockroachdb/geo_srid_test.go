package conncockroachdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// The snapshot gives geometry / geography values the SRIDs CDC gives them: a constrained column its type's SRID,
// and no SRID otherwise (geography then defaults to 4326 on the target, as CDC writes it).
func TestAlignGeoSRIDs(t *testing.T) {
	point4326, ok := datatypes.MakeGeoTypmod("Point,4326")
	require.True(t, ok)
	pointNoSRID, ok := datatypes.MakeGeoTypmod("Point")
	require.True(t, ok)
	tableSchema := &protos.TableSchema{Columns: []*protos.FieldDescription{
		{Name: "id", Type: string(types.QValueKindInt64)},
		{Name: "p", Type: string(types.QValueKindGeometry), TypeModifier: point4326},
		{Name: "shape_only", Type: string(types.QValueKindGeometry), TypeModifier: pointNoSRID},
		{Name: "g", Type: string(types.QValueKindGeometry), TypeModifier: -1},
		{Name: "gg", Type: string(types.QValueKindGeography), TypeModifier: -1},
	}}
	constrained := constrainedGeoSRIDs(tableSchema)
	require.Equal(t, map[string]int{"p": 4326}, constrained, "only columns whose type has an SRID")

	schema := types.QRecordSchema{Fields: []types.QField{
		{Name: "id", Type: types.QValueKindInt64}, {Name: "p", Type: types.QValueKindGeometry},
		{Name: "shape_only", Type: types.QValueKindGeometry}, {Name: "g", Type: types.QValueKindGeometry},
		{Name: "gg", Type: types.QValueKindGeography},
	}}
	record := []types.QValue{
		types.QValueInt64{Val: 1}, types.QValueGeometry{Val: "POINT(1 2)"},
		types.QValueGeometry{Val: "SRID=3857;POINT(1 2)"}, types.QValueGeometry{Val: "SRID=3857;POINT(1 2)"},
		types.QValueGeography{Val: "SRID=4269;POINT(1 2)"},
	}
	alignGeoSRIDs(record, schema, constrained)
	require.Equal(t, []types.QValue{
		types.QValueInt64{Val: 1}, types.QValueGeometry{Val: "SRID=4326;POINT(1 2)"},
		types.QValueGeometry{Val: "POINT(1 2)"}, types.QValueGeometry{Val: "POINT(1 2)"},
		types.QValueGeography{Val: "POINT(1 2)"},
	}, record)
	require.Nil(t, constrainedGeoSRIDs(&protos.TableSchema{}))
}
