package shared

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestArrayCastElements(t *testing.T) {
	ints, err := ArrayCastElements[int32]([]any{int32(1), nil, int32(-3)})
	require.NoError(t, err)
	require.Equal(t, []int32{1, 0, -3}, ints, "NULL elements become the zero value")

	empty, err := ArrayCastElements[string]([]any{})
	require.NoError(t, err)
	require.Empty(t, empty)

	for _, tc := range []struct {
		name string
		cast func() error
		want exceptions.ArrayElementTypeError
	}{
		// pgx decodes oid as uint32, which an int64 kind used to turn into 0
		{"oid", func() error { _, err := ArrayCastElements[int64]([]any{uint32(1)}); return err },
			exceptions.ArrayElementTypeError{Index: 0, Got: "uint32", Want: "int64"}},
		// INT8[] data under a stored INT4[] kind
		{"stale kind", func() error { _, err := ArrayCastElements[int32]([]any{int32(1), int64(2)}); return err },
			exceptions.ArrayElementTypeError{Index: 1, Got: "int64", Want: "int32"}},
		// an infinity date element pgx decodes as pgtype.InfinityModifier
		{"infinity", func() error {
			_, err := ArrayCastElements[time.Time]([]any{time.Time{}, nil, pgtype.Infinity})
			return err
		}, exceptions.ArrayElementTypeError{Index: 2, Got: "pgtype.InfinityModifier", Want: "time.Time"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var arrayErr *exceptions.ArrayElementTypeError
			require.True(t, errors.As(tc.cast(), &arrayErr))
			require.Equal(t, tc.want, *arrayErr)
		})
	}
}

func TestArrayElementTypeErrorMessage(t *testing.T) {
	err := exceptions.NewArrayElementTypeError(3, int64(1), int32(0)).WithColumn("ids")
	require.Equal(t, "array column ids: element 3 is int64, expected int32; the column's type probably changed"+
		" since the mirror read its schema: resync the mirror, or exclude the column", err.Error())
	require.Equal(t, "array column ids: element 3 is int64, expected int32; "+exceptions.HintUnexpected,
		exceptions.NewArrayElementTypeError(3, int64(1), int32(0)).WithColumn("ids").WithHint(exceptions.HintUnexpected).Error())
	require.Equal(t, "ids", err.WithColumn("other").Column, "the first column name set stays")
}
