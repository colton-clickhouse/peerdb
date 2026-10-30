package model

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
)

// parsePeerDBInterval reads the PeerDBInterval JSON that QValueInterval carries. Seconds are rounded to
// microseconds, Postgres's precision.
func parsePeerDBInterval(s string) (pgtype.Interval, error) {
	var iv datatypes.PeerDBInterval
	if err := json.Unmarshal([]byte(s), &iv); err != nil {
		return pgtype.Interval{}, fmt.Errorf("failed to parse interval %q: %w", s, err)
	}
	if !iv.Valid {
		return pgtype.Interval{}, fmt.Errorf("invalid interval %q", s)
	}
	// CockroachDB intervals have 64-bit months and days, Postgres 32-bit ones
	months, days := int64(iv.Years)*12+int64(iv.Months), int64(iv.Days)
	if months != int64(int32(months)) || days != int64(int32(days)) {
		return pgtype.Interval{}, fmt.Errorf("interval %q is out of range for a Postgres interval", s)
	}
	return pgtype.Interval{
		Months:       int32(months),
		Days:         int32(days),
		Microseconds: int64(iv.Hours)*3_600_000_000 + int64(iv.Minutes)*60_000_000 + int64(math.Round(iv.Seconds*1e6)),
		Valid:        true,
	}, nil
}

// pgIntervalText is iv as Postgres prints it (IntervalStyle postgres, as interval::text does): "1 year 2 mons
// -3 days +04:05:06.5". Postgres reads it back exactly.
func pgIntervalText(iv pgtype.Interval) string {
	// Postgres 17's infinite intervals are every field at its maximum or minimum; negating the minimum overflows
	switch {
	case iv.Months == math.MaxInt32 && iv.Days == math.MaxInt32 && iv.Microseconds == math.MaxInt64:
		return "infinity"
	case iv.Months == math.MinInt32 && iv.Days == math.MinInt32 && iv.Microseconds == math.MinInt64:
		return "-infinity"
	}
	var b strings.Builder
	negBefore := false
	field := func(n int64, unit string) {
		if n == 0 {
			return
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		if negBefore && n > 0 {
			b.WriteByte('+')
		}
		b.WriteString(strconv.FormatInt(n, 10))
		b.WriteByte(' ')
		b.WriteString(unit)
		if n != 1 {
			b.WriteByte('s')
		}
		negBefore = n < 0
	}
	field(int64(iv.Months/12), "year")
	field(int64(iv.Months%12), "mon")
	field(int64(iv.Days), "day")
	if us := iv.Microseconds; us != 0 || b.Len() == 0 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		if us < 0 {
			b.WriteByte('-')
			us = -us
		} else if negBefore {
			b.WriteByte('+')
		}
		fmt.Fprintf(&b, "%02d:%02d:%02d", us/3_600_000_000, us/60_000_000%60, us/1_000_000%60)
		if frac := us % 1_000_000; frac != 0 {
			b.WriteString(strings.TrimRight(fmt.Sprintf(".%06d", frac), "0"))
		}
	}
	return b.String()
}

// pgIntervalJSONText converts PeerDBInterval JSON to Postgres interval text for the CDC raw table; "" is an
// unmarked NULL array element.
func pgIntervalJSONText(s string) (any, error) {
	if s == "" {
		return nil, nil
	}
	iv, err := parsePeerDBInterval(s)
	if err != nil {
		return nil, err
	}
	return pgIntervalText(iv), nil
}

// pgInterval is an interval for COPY. pgx picks the encoding by the target column: an interval column gets
// the interval, a text column (pre-created, or added by a schema change) its Postgres text, as CDC writes it.
type pgInterval struct {
	pgtype.Interval
}

func (v pgInterval) TextValue() (pgtype.Text, error) {
	return pgtype.Text{String: pgIntervalText(v.Interval), Valid: true}, nil
}

func newPgInterval(s string) (pgInterval, error) {
	iv, err := parsePeerDBInterval(s)
	if err != nil {
		return pgInterval{}, err
	}
	return pgInterval{iv}, nil
}

// pgIntervalArray is an INTERVAL[] for COPY, with NULL elements where nulls says so or the element is "".
func pgIntervalArray(v []string, nulls []bool) (*pgtype.Array[*pgInterval], error) {
	elems := make([]*pgInterval, len(v))
	for i, s := range v {
		if (nulls != nil && nulls[i]) || s == "" {
			continue
		}
		iv, err := newPgInterval(s)
		if err != nil {
			return nil, err
		}
		elems[i] = &iv
	}
	return constructArray(elems), nil
}

// pgIntervalJSONArray is pgIntervalJSONText for each element.
func pgIntervalJSONArray(v []string, nulls []bool) ([]any, error) {
	out := make([]any, len(v))
	for i, s := range v {
		if nulls != nil && nulls[i] {
			continue
		}
		text, err := pgIntervalJSONText(s)
		if err != nil {
			return nil, err
		}
		out[i] = text
	}
	return out, nil
}
