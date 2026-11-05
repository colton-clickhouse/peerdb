package conncockroachdb

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func specialField(kind types.QValueKind, precision int16, nullable bool) types.QField {
	return types.QField{Name: "col", Type: kind, Precision: precision, Nullable: nullable}
}

// Snapshot path: pgx decodes NaN / infinite numerics into pgtype.Numeric and infinite dates and timestamps into
// pgtype.InfinityModifier.
func TestQValueFromCrdbValueSpecialValues(t *testing.T) {
	typeMap := pgtype.NewMap()
	nan := pgtype.Numeric{NaN: true, Valid: true}
	inf := pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}
	negInf := pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	five := pgtype.Numeric{Int: big.NewInt(5), Valid: true}
	convert := func(field types.QField, value any, special bool) types.QValue {
		t.Helper()
		qv, err := qvalueFromCrdbValue(field, protos.DBType_POSTGRES, 0, typeMap, value, special)
		require.NoError(t, err)
		return qv
	}

	numeric := specialField(types.QValueKindNumeric, 0, true)
	require.Equal(t, types.QValueNumeric{Special: types.SpecialNaN}, convert(numeric, nan, true))
	require.Equal(t, types.QValueNumeric{Special: types.SpecialNegInfinity}, convert(numeric, negInf, true))
	// numeric(p, s) can't hold infinity on Postgres: the old fallback
	bounded := specialField(types.QValueKindNumeric, 10, true)
	require.Equal(t, types.QValueNumeric{Precision: 10, Special: types.SpecialNaN}, convert(bounded, nan, true))
	require.Equal(t, types.QValueNull(types.QValueKindNumeric), convert(bounded, inf, true))
	// older mirrors and other destinations
	require.Equal(t, types.QValueNull(types.QValueKindNumeric), convert(numeric, nan, false))

	for _, kind := range []types.QValueKind{types.QValueKindDate, types.QValueKindTimestamp, types.QValueKindTimestampTZ} {
		field := specialField(kind, 0, true)
		qv := convert(field, pgtype.NegativeInfinity, true)
		require.Equal(t, kind, qv.Kind())
		require.Equal(t, types.QValueNull(kind), convert(field, pgtype.Infinity, false))
	}
	require.Equal(t, types.QValueTimestampTZ{Special: types.SpecialInfinity},
		convert(specialField(types.QValueKindTimestampTZ, 0, false), pgtype.Infinity, true))

	arr := convert(specialField(types.QValueKindArrayNumeric, 0, true), []any{nan, inf, nil, five}, true).(types.QValueArrayNumeric)
	require.Equal(t, []types.SpecialValue{types.SpecialNaN, types.SpecialInfinity, 0, 0}, arr.Specials)
	require.Equal(t, "5", arr.Val[3].String())
	plain := convert(specialField(types.QValueKindArrayNumeric, 0, true), []any{five}, true).(types.QValueArrayNumeric)
	require.Nil(t, plain.Specials)

	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	dates := convert(specialField(types.QValueKindArrayDate, 0, true),
		[]any{pgtype.Infinity, day, pgtype.NegativeInfinity}, true).(types.QValueArrayDate)
	require.Equal(t, []types.SpecialValue{types.SpecialInfinity, 0, types.SpecialNegInfinity}, dates.Specials)
	require.Equal(t, day, dates.Val[1])
	old := convert(specialField(types.QValueKindArrayDate, 0, true), []any{pgtype.Infinity, day}, false).(types.QValueArrayDate)
	require.Nil(t, old.Specials)
}

// CDC path: changefeed JSON as captured from v25.4 and v26.2.
func TestQValueFromChangefeedJSONSpecialValues(t *testing.T) {
	convert := func(field types.QField, raw string, special bool) types.QValue {
		t.Helper()
		qv, err := qvalueFromChangefeedJSON(field, json.RawMessage(raw), special)
		require.NoError(t, err)
		return qv
	}

	numeric := specialField(types.QValueKindNumeric, 0, true)
	require.Equal(t, types.QValueNumeric{Special: types.SpecialNaN}, convert(numeric, `"NaN"`, true))
	require.Equal(t, types.QValueNumeric{Special: types.SpecialInfinity}, convert(numeric, `"Infinity"`, true))
	require.Equal(t, types.QValueNumeric{Special: types.SpecialNegInfinity}, convert(numeric, `"-Infinity"`, true))
	require.Equal(t, types.QValueNull(types.QValueKindNumeric),
		convert(specialField(types.QValueKindNumeric, 10, true), `"Infinity"`, true))
	require.Equal(t, types.QValueNull(types.QValueKindNumeric), convert(numeric, `"NaN"`, false))

	require.Equal(t, types.QValueDate{Special: types.SpecialInfinity},
		convert(specialField(types.QValueKindDate, 0, true), `"infinity"`, true))
	require.Equal(t, types.QValueDate{Special: types.SpecialNegInfinity},
		convert(specialField(types.QValueKindDate, 0, true), `"-infinity"`, true))
	require.Equal(t, types.QValueTimestamp{Special: types.SpecialInfinity},
		convert(specialField(types.QValueKindTimestamp, 0, true), `"294277-01-01T23:59:59.999999"`, true))
	require.Equal(t, types.QValueTimestampTZ{Special: types.SpecialNegInfinity},
		convert(specialField(types.QValueKindTimestampTZ, 0, true), `"-4714-11-23T00:00:00Z"`, true))
	// the largest real timestamp is not infinity
	require.Equal(t, types.QValueTimestamp{Val: time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)},
		convert(specialField(types.QValueKindTimestamp, 0, true), `"294276-12-31T23:59:59.999999"`, true))

	arr := convert(specialField(types.QValueKindArrayNumeric, 0, true), `["NaN", "Infinity", "-Infinity", null, 1.5]`, true)
	require.Equal(t, []types.SpecialValue{types.SpecialNaN, types.SpecialInfinity, types.SpecialNegInfinity, 0, 0},
		arr.(types.QValueArrayNumeric).Specials)
	require.True(t, decimal.RequireFromString("1.5").Equal(arr.(types.QValueArrayNumeric).Val[4]))

	dates := convert(specialField(types.QValueKindArrayDate, 0, true), `["infinity", "-infinity", null, "2024-01-01"]`, true)
	require.Equal(t, []types.SpecialValue{types.SpecialInfinity, types.SpecialNegInfinity, 0, 0},
		dates.(types.QValueArrayDate).Specials)
	tz := convert(specialField(types.QValueKindArrayTimestampTZ, 0, true),
		`["294277-01-01T23:59:59.999999Z", "-4714-11-23T00:00:00Z", "2024-01-01T00:00:00Z"]`, true)
	require.Equal(t, []types.SpecialValue{types.SpecialInfinity, types.SpecialNegInfinity, 0},
		tz.(types.QValueArrayTimestampTZ).Specials)
	plain := convert(specialField(types.QValueKindArrayTimestamp, 0, true), `["2024-01-01T00:00:00"]`, true)
	require.Nil(t, plain.(types.QValueArrayTimestamp).Specials)

	// older mirrors: an infinity date still fails to parse, as before
	_, err := qvalueFromChangefeedJSON(specialField(types.QValueKindDate, 0, true), json.RawMessage(`"infinity"`), false)
	require.Error(t, err)
}

// The changefeed renders an infinite timestamp as the bounds of CockroachDB's range, at full precision whatever the
// column's (captured on v25.4 for TIMESTAMP(3) and TIMESTAMPTZ(0)); both are matched as prefixes.
func TestChangefeedTimeSpecialWithPrecision(t *testing.T) {
	for raw, want := range map[string]types.SpecialValue{
		`"294277-01-01T23:59:59.999999"`:  types.SpecialInfinity,
		`"294277-01-01T23:59:59.999999Z"`: types.SpecialInfinity,
		`"-4714-11-23T00:00:00"`:          types.SpecialNegInfinity,
		`"-4714-11-23T00:00:00Z"`:         types.SpecialNegInfinity,
		`"infinity"`:                      types.SpecialInfinity,
		`"2024-01-02T03:04:05.123"`:       types.SpecialNone,
	} {
		require.Equal(t, want, changefeedTimeSpecial(json.RawMessage(raw)), raw)
	}
}
