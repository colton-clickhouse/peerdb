package conncockroachdb

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// Without a server: a DATE[] with +-infinity and NULL through the snapshot conversion (pgx-decoded elements) and the
// changefeed conversion (its JSON), for each destination and version that behave differently.
func TestTimeArrayByVersionAndDestination(t *testing.T) {
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	epoch := time.Unix(0, 0).UTC()
	field := types.QField{Name: "d", Type: types.QValueKindArrayDate, Nullable: true}
	latest, old := shared.InternalVersion_Latest, shared.InternalVersion_ArrayConversionIntegrity-1
	marks := []types.SpecialValue{types.SpecialNone, types.SpecialInfinity, types.SpecialNegInfinity, types.SpecialNone}

	for _, tc := range []struct {
		name    string
		dst     protos.DBType
		version uint32
		want    types.QValue
		cdcErr  bool // the changefeed can't parse infinity for this destination (the old behaviour)
	}{
		{"postgres", protos.DBType_POSTGRES, latest,
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}, Specials: marks}, false},
		{"postgres old", protos.DBType_POSTGRES, old,
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}, Specials: marks}, false},
		{"clickhouse", protos.DBType_CLICKHOUSE, latest,
			types.QValueArrayDate{Val: []time.Time{day, epoch, epoch, epoch}}, false},
		{"clickhouse old", protos.DBType_CLICKHOUSE, old,
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}}, true},
		// a destination whose DefaultTime is the zero time
		{"snowflake", protos.DBType_SNOWFLAKE, latest,
			types.QValueArrayDate{Val: []time.Time{day, {}, {}, {}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pg := newPgValues(tc.version, tc.dst)
			snap, err := qvalueFromCrdbValue(field, tc.dst, pgtype.DateArrayOID, pgtype.NewMap(),
				[]any{day, pgtype.Infinity, pgtype.NegativeInfinity, nil}, pg)
			require.NoError(t, err)
			require.Equal(t, tc.want, snap, "snapshot")

			cdc, err := qvalueFromChangefeedJSON(field, json.RawMessage(`["2024-01-02", "infinity", "-infinity", null]`), pg)
			if tc.cdcErr {
				require.Error(t, err, "older mirrors fail on infinity in the changefeed, as before")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cdc, "changefeed")
		})
	}
}

// Only OID[] values are widened from uint32; any other array decoded as uint32 under an int64 kind is a mismatch.
func TestOIDWideningOnlyForOIDArrays(t *testing.T) {
	field := types.QField{Name: "o", Type: types.QValueKindArrayInt64, Nullable: true}
	pg := newPgValues(shared.InternalVersion_Latest, protos.DBType_POSTGRES)
	qv, err := qvalueFromCrdbValue(field, protos.DBType_POSTGRES, pgtype.OIDArrayOID, pgtype.NewMap(),
		[]any{uint32(1), uint32(4294967295)}, pg)
	require.NoError(t, err)
	require.Equal(t, types.QValueArrayInt64{Val: []int64{1, 4294967295}}, qv)

	_, err = qvalueFromCrdbValue(field, protos.DBType_POSTGRES, pgtype.Int8ArrayOID, pgtype.NewMap(), []any{uint32(1)}, pg)
	var arrayErr *exceptions.ArrayElementTypeError
	require.True(t, errors.As(err, &arrayErr), "%v", err)
	require.Equal(t, "uint32", arrayErr.Got)
}

func TestTruncateForErrorKeepsUTF8(t *testing.T) {
	short := `"abc"`
	require.Equal(t, short, truncateForError(short))
	long := `"a` + strings.Repeat("é", 40) + `"` // 2-byte runes: a byte cut at 61 would split one
	got := truncateForError(long)
	require.True(t, utf8.ValidString(got), "%q", got)
	require.LessOrEqual(t, len(got), 64)
	require.True(t, strings.HasSuffix(got, "..."))
	require.True(t, utf8.ValidString(truncateForError("\xc3\x28 invalid")), "invalid input is made valid")
}
