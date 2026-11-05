package model

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var (
	specialDay     = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	specialMarks   = []types.SpecialValue{types.SpecialNaN, types.SpecialInfinity, types.SpecialNegInfinity, 0, 0}
	specialTimes   = []types.SpecialValue{types.SpecialInfinity, types.SpecialNegInfinity, 0, 0}
	specialNulls   = []bool{false, false, false, true, false}
	specialTsNulls = []bool{false, false, true, false}
)

func specialValueRecord() []types.QValue {
	return []types.QValue{
		types.QValueNumeric{Special: types.SpecialNaN},
		types.QValueNumeric{Special: types.SpecialNegInfinity},
		types.QValueNumeric{Val: decimal.RequireFromString("1.5")},
		types.QValueDate{Special: types.SpecialInfinity},
		types.QValueTimestamp{Special: types.SpecialNegInfinity},
		types.QValueTimestampTZ{Special: types.SpecialInfinity},
		types.QValueArrayNumeric{
			Val:   []decimal.Decimal{{}, {}, {}, {}, decimal.RequireFromString("1.5")},
			Nulls: specialNulls, Specials: specialMarks,
		},
		types.QValueArrayDate{Val: []time.Time{{}, {}, {}, specialDay}, Nulls: specialTsNulls, Specials: specialTimes},
		types.QValueArrayTimestamp{Val: []time.Time{{}, {}, {}, specialDay}, Nulls: specialTsNulls, Specials: specialTimes},
		types.QValueArrayTimestampTZ{Val: []time.Time{{}, {}, {}, specialDay}, Nulls: specialTsNulls, Specials: specialTimes},
	}
}

// The COPY values encode to what Postgres stores for them.
func TestCopyFromSpecialValues(t *testing.T) {
	stream := NewQRecordStream(1)
	stream.Records <- specialValueRecord()
	src := NewQRecordCopyFromSource(stream, PgCopyOptions{})
	require.True(t, src.Next())
	values, err := src.Values()
	require.NoError(t, err)

	typeMap := pgtype.NewMap()
	oids := []uint32{
		pgtype.NumericOID, pgtype.NumericOID, pgtype.NumericOID, pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID,
		pgtype.NumericArrayOID, pgtype.DateArrayOID, pgtype.TimestampArrayOID, pgtype.TimestamptzArrayOID,
	}
	expected := []string{
		"NaN", "-Infinity", "1.5", "infinity", "-infinity", "infinity",
		"{NaN,Infinity,-Infinity,NULL,1.5}", "{infinity,-infinity,NULL,2024-01-02}",
		// pgx's text form doesn't quote the elements (COPY uses the binary form)
		`{infinity,-infinity,NULL,2024-01-02 00:00:00}`, `{infinity,-infinity,NULL,2024-01-02 00:00:00Z}`,
	}
	for i, v := range values {
		text, err := typeMap.Encode(oids[i], pgtype.TextFormatCode, v, nil)
		require.NoError(t, err, "value %d (%T)", i, v)
		require.Equal(t, expected[i], string(text), "value %d", i)
		_, err = typeMap.Encode(oids[i], pgtype.BinaryFormatCode, v, nil)
		require.NoError(t, err, "value %d (%T) as binary", i, v)
	}
}

func TestToJSONSpecialValues(t *testing.T) {
	items := NewRecordItems(10)
	for i, qv := range specialValueRecord() {
		items.AddColumn(string(rune('a'+i)), qv)
	}
	items.AddColumn("f8", types.QValueFloat64{Val: math.NaN()})
	items.AddColumn("f4", types.QValueFloat32{Val: float32(math.Inf(-1))})
	items.AddColumn("af", types.QValueArrayFloat64{Val: []float64{math.Inf(1), 1.5}})

	decode := func(opts ToJSONOptions) map[string]json.RawMessage {
		s, err := items.ToJSONWithOptions(opts)
		require.NoError(t, err)
		var m map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(s), &m))
		return m
	}
	m := decode(ToJSONOptions{ArrayNullElements: true, NonFiniteFloats: true})
	for col, want := range map[string]string{
		"a": `"NaN"`, "b": `"-Infinity"`, "c": `"1.5"`, "d": `"Infinity"`, "e": `"-Infinity"`, "f": `"Infinity"`,
		"g":  `["NaN","Infinity","-Infinity",null,"1.5"]`,
		"h":  `["Infinity","-Infinity",null,"2024-01-02"]`,
		"i":  `["Infinity","-Infinity",null,"2024-01-02T00:00:00Z"]`,
		"j":  `["Infinity","-Infinity",null,"2024-01-02T00:00:00Z"]`,
		"f8": `"NaN"`, "f4": `"-Infinity"`, "af": `["Infinity",1.5]`,
	} {
		require.JSONEq(t, want, string(m[col]), col)
	}
	// without NonFiniteFloats, non-finite floats stay JSON null
	m = decode(ToJSONOptions{})
	require.JSONEq(t, `null`, string(m["f8"]))
	require.JSONEq(t, `[null,1.5]`, string(m["af"]))
}
