package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// with PgBCDates, years before 1 AD are written as Postgres reads them (expected texts are accepted by Postgres 18).
func TestToJSONPgBCDates(t *testing.T) {
	bc := time.Date(-43, 3, 15, 12, 0, 0, 500_000_000, time.UTC) // 44 BC
	y0 := time.Date(0, 12, 31, 23, 59, 59, 999_999_000, time.UTC) // 1 BC
	ad := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	items := NewRecordItems(7)
	items.AddColumn("d", types.QValueDate{Val: bc})
	items.AddColumn("ts", types.QValueTimestamp{Val: bc})
	items.AddColumn("tz", types.QValueTimestampTZ{Val: y0})
	items.AddColumn("da", types.QValueArrayDate{Val: []time.Time{bc, ad, {}}, Nulls: []bool{false, false, true}})
	items.AddColumn("tsa", types.QValueArrayTimestamp{Val: []time.Time{time.Date(-4712, 1, 1, 0, 0, 0, 0, time.UTC), ad}})
	items.AddColumn("tza", types.QValueArrayTimestampTZ{Val: []time.Time{y0, {}}, Nulls: []bool{false, true}})
	items.AddColumn("ad", types.QValueArrayTimestamp{Val: []time.Time{ad}})

	s, err := items.ToJSONWithOptions(ToJSONOptions{PgBCDates: true, ArrayNullElements: true})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"d": "0044-03-15 BC",
		"ts": "0044-03-15 12:00:00.5 BC",
		"tz": "0001-12-31 23:59:59.999999+0000 BC",
		"da": ["0044-03-15 BC", "2026-01-01", null],
		"tsa": ["4713-01-01 00:00:00 BC", "2026-01-01 00:00:00"],
		"tza": ["0001-12-31 23:59:59.999999+0000 BC", null],
		"ad": ["2026-01-01T00:00:00Z"]
	}`, s, "arrays without a BC element keep time.Time's encoding")
}

// Without PgBCDates (other destinations, older mirrors) the output is unchanged: astronomical years, and a timestamp
// array with a year before 0 fails to marshal.
func TestToJSONPgBCDatesOff(t *testing.T) {
	bc := time.Date(-43, 3, 15, 12, 0, 0, 0, time.UTC)
	items := NewRecordItems(2)
	items.AddColumn("d", types.QValueDate{Val: bc})
	items.AddColumn("ts", types.QValueTimestamp{Val: bc})
	s, err := items.ToJSONWithOptions(ToJSONOptions{})
	require.NoError(t, err)
	require.JSONEq(t, `{"d": "-0043-03-15", "ts": "-0043-03-15 12:00:00"}`, s)

	items.AddColumn("tsa", types.QValueArrayTimestamp{Val: []time.Time{bc}})
	_, err = items.ToJSONWithOptions(ToJSONOptions{})
	require.ErrorContains(t, err, "year outside of range")
}
