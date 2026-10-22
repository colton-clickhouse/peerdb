package model

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// expected texts are Postgres 18's interval::text for the same values.
func TestPgIntervalText(t *testing.T) {
	const hour = 3_600_000_000
	for _, tc := range []struct {
		iv   pgtype.Interval
		want string
	}{
		{pgtype.Interval{Months: 14, Days: 3, Microseconds: 4*hour + 5*60_000_000 + 6_789_012}, "1 year 2 mons 3 days 04:05:06.789012"},
		{pgtype.Interval{Months: -2136000000}, "-178000000 years"},
		{pgtype.Interval{Months: 1, Days: -1, Microseconds: 1}, "1 mon -1 days +00:00:00.000001"},
		{pgtype.Interval{Months: -1, Days: 1}, "-1 mons +1 day"},
		{pgtype.Interval{Days: -1, Microseconds: hour}, "-1 days +01:00:00"},
		{pgtype.Interval{}, "00:00:00"},
		{pgtype.Interval{Microseconds: -hour}, "-01:00:00"},
		{pgtype.Interval{Days: 1}, "1 day"},
		{pgtype.Interval{Months: -14}, "-1 years -2 mons"},
		{pgtype.Interval{Microseconds: 9223372036854775807}, "2562047788:00:54.775807"},
		{pgtype.Interval{Microseconds: 6_500_000}, "00:00:06.5"},
		{pgtype.Interval{Days: 106751991, Microseconds: 4*hour + 54_775_807}, "106751991 days 04:00:54.775807"},
		{pgtype.Interval{Days: -1, Microseconds: -hour}, "-1 days -01:00:00"},
	} {
		require.Equal(t, tc.want, pgIntervalText(tc.iv))
	}
}

func TestParsePeerDBInterval(t *testing.T) {
	// as the CockroachDB and Postgres connectors write it: fields split, each with the interval's sign
	iv, err := parsePeerDBInterval(`{"hours":-1,"minutes":-2,"seconds":-3.000001,"days":-4,"months":-5,"years":-6,"valid":true}`)
	require.NoError(t, err)
	require.Equal(t, pgtype.Interval{Months: -77, Days: -4, Microseconds: -3_723_000_001, Valid: true}, iv)

	iv, err = parsePeerDBInterval(`{"valid":true}`)
	require.NoError(t, err)
	require.Equal(t, pgtype.Interval{Valid: true}, iv)

	_, err = parsePeerDBInterval(`{"years":300000000,"valid":true}`)
	require.ErrorContains(t, err, "out of range")
	_, err = parsePeerDBInterval(`{"days":1}`)
	require.Error(t, err)
	_, err = parsePeerDBInterval(`not json`)
	require.Error(t, err)
}

// pgx encodes pgInterval by the target column: an interval for interval columns, its Postgres text for text.
func TestPgIntervalEncode(t *testing.T) {
	m := pgtype.NewMap()
	v, err := newPgInterval(`{"hours":4,"days":3,"months":2,"years":1,"valid":true}`)
	require.NoError(t, err)

	buf, err := m.Encode(pgtype.IntervalOID, pgtype.BinaryFormatCode, v, nil)
	require.NoError(t, err)
	var back pgtype.Interval
	require.NoError(t, m.Scan(pgtype.IntervalOID, pgtype.BinaryFormatCode, buf, &back))
	require.Equal(t, pgtype.Interval{Months: 14, Days: 3, Microseconds: 4 * 3_600_000_000, Valid: true}, back)

	buf, err = m.Encode(pgtype.TextOID, pgtype.BinaryFormatCode, v, nil)
	require.NoError(t, err)
	require.Equal(t, "1 year 2 mons 3 days 04:00:00", string(buf))

	arr, err := pgIntervalArray([]string{`{"days":1,"valid":true}`, "", `{"days":2,"valid":true}`}, []bool{false, false, true})
	require.NoError(t, err)
	buf, err = m.Encode(pgtype.IntervalArrayOID, pgtype.BinaryFormatCode, arr, nil)
	require.NoError(t, err)
	var backArr []*pgtype.Interval
	require.NoError(t, m.Scan(pgtype.IntervalArrayOID, pgtype.BinaryFormatCode, buf, &backArr))
	require.Len(t, backArr, 3)
	require.Equal(t, pgtype.Interval{Days: 1, Valid: true}, *backArr[0])
	require.Nil(t, backArr[1])
	require.Nil(t, backArr[2])

	buf, err = m.Encode(pgtype.TextArrayOID, pgtype.BinaryFormatCode, arr, nil)
	require.NoError(t, err)
	var texts []*string
	require.NoError(t, m.Scan(pgtype.TextArrayOID, pgtype.BinaryFormatCode, buf, &texts))
	require.Equal(t, "1 day", *texts[0])
	require.Nil(t, texts[1])
}

func TestToJSONPgIntervals(t *testing.T) {
	items := NewRecordItems(2)
	items.AddColumn("i", types.QValueInterval{Val: `{"hours":-1,"days":2,"valid":true}`})
	items.AddColumn("a", types.QValueArrayInterval{
		Val: []string{`{"months":1,"valid":true}`, "", `{"valid":true}`}, Nulls: []bool{false, true, false},
	})

	for _, tc := range []struct {
		opts ToJSONOptions
		want string
	}{
		{ToJSONOptions{PgIntervals: true, ArrayNullElements: true}, `{"a":["1 mon",null,"00:00:00"],"i":"2 days -01:00:00"}`},
		{ToJSONOptions{}, `{"a":["{\"months\":1,\"valid\":true}","","{\"valid\":true}"],"i":"{\"hours\":-1,\"days\":2,\"valid\":true}"}`},
	} {
		s, err := items.ToJSONWithOptions(tc.opts)
		require.NoError(t, err)
		require.JSONEq(t, tc.want, s)
	}
	var m map[string]any
	s, err := items.ToJSONWithOptions(ToJSONOptions{PgIntervals: true})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(s), &m))
	require.Equal(t, []any{"1 mon", nil, "00:00:00"}, m["a"], "NULL elements stay NULL without ArrayNullElements too")
}
