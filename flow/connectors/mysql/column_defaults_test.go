package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestDefaultExprFromInfoSchema(t *testing.T) {
	for _, tc := range []struct {
		name          string
		qkind         types.QValueKind
		columnDefault string
		extra         string
		mariaDB       bool
		want          string // "" for none
	}{
		{"int", types.QValueKindInt32, "5", "", false, "5"},
		{"negative decimal", types.QValueKindNumeric, "-1.50", "", false, "-1.50"},
		{"string", types.QValueKindString, "it's", "", false, "'it''s'"},
		{"string that looks like a function", types.QValueKindString, "now()", "", false, ""},
		{"empty string", types.QValueKindString, "", "", false, "''"},
		{"tinyint(1)", types.QValueKindBoolean, "1", "", false, "1"},
		{"bit literal", types.QValueKindUInt64, "b'101'", "", false, "5"},
		{"datetime literal", types.QValueKindTimestamp, "2020-01-02 03:04:05", "", false, "'2020-01-02 03:04:05'"},
		{"current timestamp", types.QValueKindTimestamp, "CURRENT_TIMESTAMP", "DEFAULT_GENERATED", false, "CURRENT_TIMESTAMP"},
		{"current timestamp(6)", types.QValueKindTimestamp, "CURRENT_TIMESTAMP(6)", "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)",
			false, "CURRENT_TIMESTAMP"},
		{"expression", types.QValueKindString, "uuid()", "DEFAULT_GENERATED", false, ""},
		{"backslash", types.QValueKindString, `a\b`, "", false, ""},
		{"enum ordinal", types.QValueKindUint16Enum, "a", "", false, ""},
		{"json", types.QValueKindJSON, "{}", "", false, ""},
		{"MariaDB string", types.QValueKindString, "'it''s'", "", true, "'it''s'"},
		{"MariaDB number", types.QValueKindInt32, "5", "", true, "5"},
		{"MariaDB NULL", types.QValueKindString, "NULL", "", true, ""},
		{"MariaDB current_timestamp()", types.QValueKindTimestamp, "current_timestamp()", "", true, "CURRENT_TIMESTAMP"},
		{"MariaDB expression", types.QValueKindString, "uuid()", "", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := defaultExprFromInfoSchema(tc.qkind, tc.columnDefault, tc.extra, tc.mariaDB)
			require.Equal(t, tc.want != "", ok)
			require.Equal(t, tc.want, got)
		})
	}
}
