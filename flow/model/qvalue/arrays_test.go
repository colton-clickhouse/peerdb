package qvalue

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestPrepareTimeArray(t *testing.T) {
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	epoch := time.Unix(0, 0).UTC()
	in := []any{day, pgtype.Infinity, nil, pgtype.NegativeInfinity}

	out, specials := PrepareTimeArray(in, TimeArrayOptions{Specials: true, DefaultTime: true, Destination: protos.DBType_POSTGRES})
	require.Equal(t, []any{day, time.Time{}, nil, time.Time{}}, out, "NULL stays nil for the NULL mask")
	require.Equal(t, []types.SpecialValue{0, types.SpecialInfinity, 0, types.SpecialNegInfinity}, specials)

	out, specials = PrepareTimeArray(in, TimeArrayOptions{DefaultTime: true, Destination: protos.DBType_CLICKHOUSE})
	require.Equal(t, []any{day, epoch, epoch, epoch}, out)
	require.Nil(t, specials)

	out, specials = PrepareTimeArray(in, TimeArrayOptions{Destination: protos.DBType_CLICKHOUSE})
	require.Equal(t, []any{day, time.Time{}, nil, time.Time{}}, out, "older mirrors: the zero time")
	require.Nil(t, specials)

	require.Equal(t, []any{day, pgtype.Infinity, nil, pgtype.NegativeInfinity}, in, "the input isn't modified")
	plain := []any{day, day}
	out, _ = PrepareTimeArray(plain, TimeArrayOptions{DefaultTime: true, Destination: protos.DBType_CLICKHOUSE})
	require.Same(t, &plain[0], &out[0], "nothing to replace: no copy")
}

func TestNumericSpecial(t *testing.T) {
	require.Equal(t, types.SpecialNaN, NumericSpecial(pgtype.Numeric{NaN: true, Valid: true}, 10))
	require.Equal(t, types.SpecialInfinity, NumericSpecial(pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, 0))
	require.Equal(t, types.SpecialNone, NumericSpecial(pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, 10),
		"numeric(p, s) can't hold infinity")
	require.Equal(t, types.SpecialNone, NumericSpecial(pgtype.Numeric{}, 0))
}
