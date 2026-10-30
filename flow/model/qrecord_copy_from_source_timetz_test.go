package model

import (
	"encoding/binary"
	"github.com/jackc/pgx/v5/pgtype"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimetzBinary(t *testing.T) {
	// 12:34:56.789 UTC: int64 microseconds, then a zero zone
	require.Equal(t,
		[]byte{0, 0, 0, 0x0a, 0x8b, 0xe6, 0x26, 0x08, 0, 0, 0, 0},
		timetzBinary(12*time.Hour+34*time.Minute+56*time.Second+789*time.Millisecond))
}

// A Postgres source's TIMETZ arrives as its UTC offset from the start of the day, which can leave the day: 01:00+05 is
// -4h and 23:30-05 is 28:30 (the CockroachDB parser wraps; the Postgres one doesn't). timetz_recv takes only 0 to
// 24:00, so the encoder wraps.
func TestTimetzBinaryWrapsIntoTheDay(t *testing.T) {
	micros := func(b []byte) time.Duration { return time.Duration(binary.BigEndian.Uint64(b[:8])) * time.Microsecond }
	require.Equal(t, 20*time.Hour, micros(timetzBinary(-4*time.Hour)))
	require.Equal(t, 4*time.Hour+30*time.Minute, micros(timetzBinary(28*time.Hour+30*time.Minute)))
	require.Equal(t, 24*time.Hour, micros(timetzBinary(24*time.Hour)), "24:00 is a valid timetz")
	require.Zero(t, micros(timetzBinary(0)))
	require.Equal(t, 23*time.Hour+59*time.Minute, micros(timetzBinary(-time.Minute)))
}

// pgx encodes a timetz by the target column: PG's binary timetz for timetz (an OID pgx doesn't know), the UTC time of
// day for time, and its text for text and bytea. Before, the 12 bytes went to every column: a time column had no
// encoding for them, and text or bytea columns took them raw.
func TestPgTimeTZEncodesByTarget(t *testing.T) {
	m := pgtype.NewMap()
	v := pgTimeTZ{-4 * time.Hour} // 01:00+05
	encode := func(oid uint32) []byte {
		buf, err := m.Encode(oid, pgtype.BinaryFormatCode, v, nil)
		require.NoError(t, err, "oid %d", oid)
		return buf
	}
	require.Equal(t, timetzBinary(20*time.Hour), encode(pgtype.TimetzOID))
	require.Equal(t, encodeWith(t, m, pgtype.TimeOID, pgtype.Time{Microseconds: int64(20 * time.Hour / time.Microsecond),
		Valid: true}), encode(pgtype.TimeOID))
	require.Equal(t, []byte("20:00:00.000000"), encode(pgtype.TextOID))
	require.Equal(t, []byte("20:00:00.000000"), encode(pgtype.VarcharOID))
	require.Equal(t, []byte("20:00:00.000000"), encode(pgtype.ByteaOID))
}

func encodeWith(t *testing.T, m *pgtype.Map, oid uint32, value any) []byte {
	t.Helper()
	buf, err := m.Encode(oid, pgtype.BinaryFormatCode, value, nil)
	require.NoError(t, err)
	return buf
}
