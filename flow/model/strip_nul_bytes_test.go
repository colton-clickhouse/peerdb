package model

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestStripNulBytes(t *testing.T) {
	require.Equal(t, "ab", StripNulBytes("a\x00b"))
	require.Empty(t, StripNulBytes("\x00\x00"))
	require.Equal(t, "plain", StripNulBytes("plain"))
	a := []string{"x", "y"}
	require.Equal(t, a, stripNulBytesAll(a))
	require.Equal(t, []string{"x", "y"}, stripNulBytesAll([]string{"\x00x", "y\x00"}))
}

func TestRecordItemsStripNulBytes(t *testing.T) {
	items := NewRecordItems(3)
	items.AddColumn("s", types.QValueString{Val: "a\x00b"})
	items.AddColumn("e", types.QValueEnum{Val: "\x00e"})
	items.AddColumn("sa", types.QValueArrayString{Val: []string{"x\x00"}})

	stripped, err := items.ToJSONWithOptions(ToJSONOptions{StripNulBytes: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"s": "ab", "e": "e", "sa": ["x"]}`, stripped)

	// other destinations, and Postgres without PEERDB_PG_STRIP_NUL_BYTES, keep the bytes
	kept, err := items.ToJSONWithOptions(ToJSONOptions{})
	require.NoError(t, err)
	require.JSONEq(t, `{"s": "a\u0000b", "e": "\u0000e", "sa": ["x\u0000"]}`, kept)
}

// The COPY source keeps NUL bytes unless the mirror opts in (PEERDB_PG_STRIP_NUL_BYTES): Postgres then refuses the
// value, so the snapshot fails loudly instead of changing data.
func TestCopyStripNulBytesOptIn(t *testing.T) {
	values := func(strip bool) []any {
		stream := NewQRecordStream(1)
		stream.SetSchema(types.QRecordSchema{Fields: []types.QField{
			{Name: "s", Type: types.QValueKindString, Nullable: true},
			{Name: "sa", Type: types.QValueKindArrayString, Nullable: true},
		}})
		stream.Records <- []types.QValue{types.QValueString{Val: "a\x00b"}, types.QValueArrayString{Val: []string{"x\x00"}}}
		src := &QRecordCopyFromSource{stream: stream}
		if strip {
			src.WithStripNulBytes()
		}
		require.True(t, src.Next())
		v, err := src.Values()
		require.NoError(t, err)
		return v
	}
	kept := values(false)
	require.Equal(t, "a\x00b", kept[0])
	require.Equal(t, []string{"x\x00"}, kept[1].(*pgtype.Array[string]).Elements)

	stripped := values(true)
	require.Equal(t, "ab", stripped[0])
	require.Equal(t, []string{"x"}, stripped[1].(*pgtype.Array[string]).Elements)
}
