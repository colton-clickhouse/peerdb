package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithArrayNulls(t *testing.T) {
	nulls := []bool{false, true, false}
	qv := WithArrayNulls(QValueArrayInt64{Val: []int64{1, 0, 3}}, nulls)
	require.Equal(t, QValueArrayInt64{Val: []int64{1, 0, 3}, Nulls: nulls}, qv)
	require.Equal(t, nulls, ArrayNulls(qv))

	// no NULL: no mask
	require.Nil(t, ArrayNulls(WithArrayNulls(QValueArrayString{Val: []string{"a"}}, []bool{false})))
	// length mismatch and non-arrays: unchanged
	require.Nil(t, ArrayNulls(WithArrayNulls(QValueArrayString{Val: []string{"a"}}, nulls)))
	require.Equal(t, QValueInt64{Val: 1}, WithArrayNulls(QValueInt64{Val: 1}, nulls))
	require.Nil(t, ArrayNulls(QValueInt64{Val: 1}))
}

func TestCheckArrayNulls(t *testing.T) {
	require.NoError(t, CheckArrayNulls(QValueArrayInt64{Val: []int64{1, 0}, Nulls: []bool{false, true}}))
	require.NoError(t, CheckArrayNulls(QValueArrayInt64{Val: []int64{1}}))
	require.NoError(t, CheckArrayNulls(QValueInt64{Val: 1}))
	require.Error(t, CheckArrayNulls(QValueArrayInt64{Val: []int64{1, 0}, Nulls: []bool{true}}))
	require.Error(t, CheckArrayNulls(QValueArrayString{Val: []string{"a"}, Nulls: []bool{false, true}}))
}
