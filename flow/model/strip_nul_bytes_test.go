package model

import (
	"testing"

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

	// other destinations keep the bytes
	kept, err := items.ToJSONWithOptions(ToJSONOptions{})
	require.NoError(t, err)
	require.JSONEq(t, `{"s": "a\u0000b", "e": "\u0000e", "sa": ["x\u0000"]}`, kept)
}
