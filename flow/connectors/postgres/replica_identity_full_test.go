package connpostgres

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// A REPLICA IDENTITY FULL table gets its source's primary key on the target, as one with the default replica
// identity does, unless the key is all the target's columns in table order (after exclusions): what a Postgres
// source reports for a keyless one, whose rows needn't be unique.
func TestCreateTableKeepsReplicaIdentityFullPrimaryKey(t *testing.T) {
	t.Parallel()

	columns := func(names ...string) []*protos.FieldDescription {
		fields := make([]*protos.FieldDescription, 0, len(names))
		for _, name := range names {
			fields = append(fields, &protos.FieldDescription{Name: name, Type: "integer", TypeModifier: -1})
		}
		return fields
	}
	const table = `CREATE TABLE IF NOT EXISTS "s"."t"(`
	for _, tc := range []struct {
		name    string
		key     []string
		full    bool
		exclude []string
		want    string
	}{
		{name: "default identity", key: []string{"a", "b"},
			want: table + `"a" integer,"b" integer,"c" integer,PRIMARY KEY("a","b"))`},
		{name: "full, keyed", key: []string{"a", "b"}, full: true,
			want: table + `"a" integer,"b" integer,"c" integer,PRIMARY KEY("a","b"))`},
		{name: "full, keyless (keyed on all columns)", key: []string{"a", "b", "c"}, full: true,
			want: table + `"a" integer,"b" integer,"c" integer)`},
		{name: "full, primary key on all columns: looks keyless", key: []string{"a", "b", "c"}, full: true,
			want: table + `"a" integer,"b" integer,"c" integer)`},
		{name: "full, primary key on all columns in another order", key: []string{"c", "a", "b"}, full: true,
			want: table + `"a" integer,"b" integer,"c" integer,PRIMARY KEY("c","a","b"))`},
		{name: "full, excluded key column", key: []string{"a", "b"}, full: true, exclude: []string{"b"},
			want: table + `"a" integer,"c" integer,PRIMARY KEY("a"))`},
		{name: "full, excluded non-key column: the key is then all columns", key: []string{"a", "b"}, full: true,
			exclude: []string{"c"}, want: table + `"a" integer,"b" integer)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tableMappings := []*protos.TableMapping{
				{SourceTableIdentifier: "src.t", DestinationTableIdentifier: "s.t", Exclude: tc.exclude},
			}
			processed := internal.BuildProcessedSchemaMapping(tableMappings, map[string]*protos.TableSchema{"src.t": {
				TableIdentifier:       "src.t",
				PrimaryKeyColumns:     tc.key,
				IsReplicaIdentityFull: tc.full,
				System:                protos.TypeSystem_PG,
				Columns:               columns("a", "b", "c"),
			}}, log.NewStructuredLogger(slog.Default()))
			sql, err := generateCreateTableSQLForNormalizedTable(
				&protos.SetupNormalizedTableBatchInput{}, &common.QualifiedTable{Namespace: "s", Table: "t"}, processed["s.t"])
			require.NoError(t, err)
			require.Equal(t, tc.want, sql)
		})
	}
}
