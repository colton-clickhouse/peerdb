package model

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Postgres before 14 rejects numeric infinity, so it's written as NULL there; NaN and float infinity are kept.
func TestWithoutNumericInfinity(t *testing.T) {
	require.Equal(t, types.QValueNull(types.QValueKindNumeric),
		WithoutNumericInfinity(types.QValueNumeric{Special: types.SpecialInfinity}))
	require.Equal(t, types.QValueNull(types.QValueKindNumeric),
		WithoutNumericInfinity(types.QValueNumeric{Special: types.SpecialNegInfinity}))
	nan := types.QValueNumeric{Special: types.SpecialNaN}
	require.Equal(t, nan, WithoutNumericInfinity(nan))
	one := types.QValueNumeric{Val: decimal.NewFromInt(1)}
	require.Equal(t, one, WithoutNumericInfinity(one))
	float := types.QValueFloat64{Val: 1}
	require.Equal(t, float, WithoutNumericInfinity(float))

	arr := types.QValueArrayNumeric{
		Val:      []decimal.Decimal{decimal.NewFromInt(1), {}, {}, {}},
		Nulls:    []bool{false, true, false, false},
		Specials: []types.SpecialValue{types.SpecialNone, types.SpecialNone, types.SpecialInfinity, types.SpecialNaN},
	}
	got := WithoutNumericInfinity(arr).(types.QValueArrayNumeric)
	require.Equal(t, []bool{false, true, true, false}, got.Nulls)
	require.Equal(t, []types.SpecialValue{types.SpecialNone, types.SpecialNone, types.SpecialNone, types.SpecialNaN},
		got.Specials)
	require.Equal(t, types.SpecialInfinity, arr.Specials[2], "the input isn't modified")

	items := NewRecordItems(2)
	items.AddColumn("n", types.QValueNumeric{Special: types.SpecialInfinity})
	items.AddColumn("a", arr)
	withInf, err := items.ToJSONWithOptions(ToJSONOptions{ArrayNullElements: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"n":"Infinity","a":["1",null,"Infinity","NaN"]}`, withInf)
	without, err := items.ToJSONWithOptions(ToJSONOptions{ArrayNullElements: true, NoNumericInfinity: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"n":null,"a":["1",null,null,"NaN"]}`, without)
}
