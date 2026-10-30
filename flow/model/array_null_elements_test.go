package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestConstructNullableArray(t *testing.T) {
	require.Equal(t, constructArray([]int64{1, 2}), constructNullableArray([]int64{1, 2}, nil))

	arr, ok := constructNullableArray([]int64{1, 0, 3}, []bool{false, true, false}).(*pgtype.Array[*int64])
	require.True(t, ok)
	require.Len(t, arr.Elements, 3)
	require.Equal(t, int64(1), *arr.Elements[0])
	require.Nil(t, arr.Elements[1])
	require.Equal(t, int64(3), *arr.Elements[2])
}

func TestToJSONArrayNullElements(t *testing.T) {
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	items := NewRecordItems(3)
	nulls := []bool{false, true, false}
	items.AddColumn("ia", types.QValueArrayInt64{Val: []int64{1, 0, 3}, Nulls: nulls})
	items.AddColumn("sa", types.QValueArrayString{Val: []string{"a", "", "c"}, Nulls: nulls})
	items.AddColumn("da", types.QValueArrayDate{Val: []time.Time{day, {}, day}, Nulls: nulls})

	decode := func(opts ToJSONOptions) map[string]json.RawMessage {
		s, err := items.ToJSONWithOptions(opts)
		require.NoError(t, err)
		var m map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(s), &m))
		return m
	}
	on := decode(ToJSONOptions{ArrayNullElements: true})
	require.JSONEq(t, `[1,null,3]`, string(on["ia"]))
	require.JSONEq(t, `["a",null,"c"]`, string(on["sa"]))
	require.JSONEq(t, `["2026-01-02",null,"2026-01-02"]`, string(on["da"]))
	// other destinations don't ask for it: zero values as before
	off := decode(ToJSONOptions{})
	require.JSONEq(t, `[1,0,3]`, string(off["ia"]))
	require.JSONEq(t, `["a","","c"]`, string(off["sa"]))
}

// A NULL-element mask without one entry per element fails the CDC JSON too, as it fails the COPY, instead of being
// dropped (which would write the placeholders as values).
func TestToJSONArrayNullMaskMismatch(t *testing.T) {
	items := NewRecordItems(1)
	items.AddColumn("a", types.QValueArrayInt64{Val: []int64{1, 0}, Nulls: []bool{true}})
	_, err := items.ToJSONWithOptions(ToJSONOptions{ArrayNullElements: true})
	require.ErrorContains(t, err, "NULL-element mask of 1 entries for 2 elements")
}
