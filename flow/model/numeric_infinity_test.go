package model

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Postgres before 14 rejects numeric infinity, so it's written as NULL there, or 0 in a column that isn't nullable;
// NaN and float infinity are kept.
func TestWithoutNumericInfinity(t *testing.T) {
	require.Equal(t, types.QValueNull(types.QValueKindNumeric),
		WithoutNumericInfinity(types.QValueNumeric{Special: types.SpecialInfinity}, true))
	require.Equal(t, types.QValueNull(types.QValueKindNumeric),
		WithoutNumericInfinity(types.QValueNumeric{Special: types.SpecialNegInfinity}, true))
	require.Equal(t, types.QValueNumeric{Val: decimal.Zero, Precision: 10, Scale: 2},
		WithoutNumericInfinity(types.QValueNumeric{Special: types.SpecialInfinity, Precision: 10, Scale: 2}, false))
	nan := types.QValueNumeric{Special: types.SpecialNaN}
	require.Equal(t, nan, WithoutNumericInfinity(nan, true))
	one := types.QValueNumeric{Val: decimal.NewFromInt(1)}
	require.Equal(t, one, WithoutNumericInfinity(one, false))
	float := types.QValueFloat64{Val: 1}
	require.Equal(t, float, WithoutNumericInfinity(float, true))

	arr := types.QValueArrayNumeric{
		Val:      []decimal.Decimal{decimal.NewFromInt(1), {}, {}, {}},
		Nulls:    []bool{false, true, false, false},
		Specials: []types.SpecialValue{types.SpecialNone, types.SpecialNone, types.SpecialInfinity, types.SpecialNaN},
	}
	got := WithoutNumericInfinity(arr, false).(types.QValueArrayNumeric)
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
	// a key or NOT NULL column gets 0, which it takes; array elements are always nullable
	notNull, err := items.ToJSONWithOptions(ToJSONOptions{
		ArrayNullElements: true, NoNumericInfinity: true, NotNullColumns: map[string]struct{}{"n": {}, "a": {}},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"n":"0","a":["1",null,null,"NaN"]}`, notNull)
}

// The COPY takes nullability from the stream's schema: a NOT NULL column gets 0, a nullable one NULL.
func TestCopyWithoutNumericInfinity(t *testing.T) {
	stream := NewQRecordStream(1)
	stream.SetSchema(types.QRecordSchema{Fields: []types.QField{
		{Name: "nullable", Type: types.QValueKindNumeric, Nullable: true},
		{Name: "not_null", Type: types.QValueKindNumeric, Nullable: false},
	}})
	inf := types.QValueNumeric{Special: types.SpecialInfinity}
	stream.Records <- []types.QValue{inf, inf}
	src := NewQRecordCopyFromSource(stream).WithoutNumericInfinity()
	require.True(t, src.Next())
	values, err := src.Values()
	require.NoError(t, err)
	require.Nil(t, values[0])
	zero, ok := values[1].(decimal.Decimal)
	require.True(t, ok, "%T", values[1])
	require.True(t, zero.IsZero())
}
