package model

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// a TIME value must encode as the same duration into interval as into time, negative values included
func TestPgDurationEncoding(t *testing.T) {
	m := pgtype.NewMap()
	for _, d := range []time.Duration{
		-(838*time.Hour + 59*time.Minute + 59*time.Second),
		-(12*time.Hour + 34*time.Minute + 56*time.Second + time.Microsecond),
		-time.Second,
		100*time.Hour + 500*time.Millisecond,
	} {
		v := pgDuration{pgtype.Time{Microseconds: int64(d / time.Microsecond), Valid: true}}

		buf, err := m.Encode(pgtype.IntervalOID, pgtype.BinaryFormatCode, v, nil)
		require.NoError(t, err, d)
		var iv pgtype.Interval
		require.NoError(t, m.Scan(pgtype.IntervalOID, pgtype.BinaryFormatCode, buf, &iv), d)
		require.Equal(t, pgtype.Interval{Microseconds: v.Microseconds, Valid: true}, iv, d)

		// into time: unchanged from pgtype.Time
		buf, err = m.Encode(pgtype.TimeOID, pgtype.BinaryFormatCode, v, nil)
		require.NoError(t, err, d)
		plain, err := m.Encode(pgtype.TimeOID, pgtype.BinaryFormatCode, v.Time, nil)
		require.NoError(t, err, d)
		require.Equal(t, plain, buf, d)
	}
}
