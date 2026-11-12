package model

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// A special-element list that doesn't match its array fails the COPY and the CDC JSON, instead of panicking or being
// dropped.
func TestArraySpecialsMismatch(t *testing.T) {
	arr := types.QValueArrayNumeric{Val: []decimal.Decimal{{}, {}}, Specials: []types.SpecialValue{types.SpecialNaN}}
	require.Error(t, types.CheckArraySpecials(arr))
	items := NewRecordItems(1)
	items.AddColumn("a", arr)
	_, err := items.ToJSONWithOptions(ToJSONOptions{})
	require.ErrorContains(t, err, "special-element list of 1 entries for 2 elements")
}
