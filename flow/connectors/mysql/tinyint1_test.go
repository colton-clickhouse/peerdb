package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestTinyInt1Kind(t *testing.T) {
	setting := func(destinationType string) *protos.ColumnSetting {
		return &protos.ColumnSetting{SourceName: "c", DestinationType: destinationType}
	}
	for _, tc := range []struct {
		name        string
		qkind       types.QValueKind
		columnType  string
		setting     *protos.ColumnSetting
		mirrorAsInt bool
		want        types.QValueKind
	}{
		{"default stays boolean", types.QValueKindBoolean, "tinyint(1)", nil, false, types.QValueKindBoolean},
		{"mirror setting", types.QValueKindBoolean, "tinyint(1)", nil, true, types.QValueKindInt8},
		{"mirror setting, unsigned", types.QValueKindBoolean, "tinyint(1) unsigned", nil, true, types.QValueKindUInt8},
		{"column setting", types.QValueKindBoolean, "tinyint(1)", setting("smallint"), false, types.QValueKindInt8},
		{"column setting, ClickHouse type", types.QValueKindBoolean, "tinyint(1)", setting("Int8"), false, types.QValueKindInt8},
		{"column setting wins", types.QValueKindBoolean, "tinyint(1)", setting("boolean"), true, types.QValueKindBoolean},
		{"other destination type keeps the mirror's", types.QValueKindBoolean, "tinyint(1)", setting("String"), true,
			types.QValueKindInt8},
		{"not tinyint(1)", types.QValueKindInt8, "tinyint(4)", setting("smallint"), true, types.QValueKindInt8},
		{"bit(1) isn't tinyint(1)", types.QValueKindBoolean, "bit(1)", nil, true, types.QValueKindBoolean},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tinyInt1Kind(tc.qkind, tc.columnType, tc.setting, tc.mirrorAsInt))
		})
	}
}

// The mirror's own pinned setting decides; a mirror without one (from before the setting) keeps booleans
func TestTinyInt1Pinned(t *testing.T) {
	pinned, asInt, err := tinyInt1Pinned(nil)
	require.NoError(t, err)
	require.False(t, pinned)
	require.False(t, asInt)
	pinned, asInt, err = tinyInt1Pinned(map[string]string{"PEERDB_MYSQL_TINYINT1_AS_INT": "true"})
	require.NoError(t, err)
	require.True(t, pinned)
	require.True(t, asInt)
	_, _, err = tinyInt1Pinned(map[string]string{"PEERDB_MYSQL_TINYINT1_AS_INT": "maybe"})
	require.Error(t, err)
}
