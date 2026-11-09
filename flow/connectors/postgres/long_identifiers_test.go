package connpostgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestLongIdentifiersError(t *testing.T) {
	long := strings.Repeat("c", 64)
	schemas := map[string]*protos.TableSchema{
		"src.t": {Columns: []*protos.FieldDescription{{Name: "id"}, {Name: long}}},
	}
	mapping := func(dst string, exclude ...string) []*protos.TableMapping {
		return []*protos.TableMapping{{SourceTableIdentifier: "src.t", DestinationTableIdentifier: dst, Exclude: exclude}}
	}

	err := longIdentifiersError(mapping("dst.t"), schemas, false)
	require.ErrorContains(t, err, "column dst.t."+long)
	// excluded columns aren't created
	require.NoError(t, longIdentifiersError(mapping("dst.t", long), schemas, false))
	// 63 bytes is the limit, counted in bytes: 32 two-byte characters are 64 bytes
	require.NoError(t, longIdentifiersError(mapping("dst."+strings.Repeat("t", 63), long), schemas, false))
	require.ErrorContains(t, longIdentifiersError(mapping("dst."+strings.Repeat("é", 32), long), schemas, false),
		"table dst."+strings.Repeat("é", 32))
}

func TestLongPeerDBNames(t *testing.T) {
	ok := strings.Repeat("m", maxIdentifierBytes-len(getRawTableIdentifier("")))
	require.NoError(t, longPeerDBNamesError(&protos.FlowConnectionConfigsCore{FlowJobName: ok}))
	err := longPeerDBNamesError(&protos.FlowConnectionConfigsCore{FlowJobName: ok + "x",
		SoftDeleteColName: strings.Repeat("d", 64), SyncedAtColName: "_PEERDB_SYNCED_AT"})
	require.ErrorContains(t, err, "mirror name "+ok+"x")
	require.ErrorContains(t, err, "soft delete column")
	require.NotContains(t, err.Error(), "synced-at column")
}

// Existing mirrors may have long names (from before the check): a resync keeps them, so it isn't refused for them
func TestLongPeerDBNamesNotOnResync(t *testing.T) {
	cfg := &protos.FlowConnectionConfigsCore{FlowJobName: strings.Repeat("m", maxIdentifierBytes), Resync: true}
	require.NoError(t, (&PostgresConnector{}).ValidateMirrorDestination(t.Context(), cfg, nil))
}

// A resync recovers an existing mirror: only names that collide once truncated refuse it, not a long name alone
func TestLongIdentifiersErrorOnResync(t *testing.T) {
	a, b := strings.Repeat("c", 63)+"a", strings.Repeat("c", 63)+"b"
	mapping := []*protos.TableMapping{{SourceTableIdentifier: "src.t", DestinationTableIdentifier: "dst.t"}}
	alone := map[string]*protos.TableSchema{"src.t": {Columns: []*protos.FieldDescription{{Name: "id"}, {Name: a}}}}
	require.NoError(t, longIdentifiersError(mapping, alone, true))
	require.Error(t, longIdentifiersError(mapping, alone, false))

	colliding := map[string]*protos.TableSchema{"src.t": {Columns: []*protos.FieldDescription{{Name: a}, {Name: b}}}}
	require.ErrorContains(t, longIdentifiersError(mapping, colliding, true), "columns dst.t."+a+", dst.t."+b)

	tables := []*protos.TableMapping{
		{SourceTableIdentifier: "src.t", DestinationTableIdentifier: "dst." + a},
		{SourceTableIdentifier: "src.u", DestinationTableIdentifier: "dst." + b},
	}
	require.ErrorContains(t, longIdentifiersError(tables, nil, true), "tables dst."+a+", dst."+b)
}

// Postgres clips a long name at a character boundary: a 64-byte name ending in a two-byte character keeps 62 bytes,
// so it collides with the 62-byte name
func TestLongIdentifiersTruncateAtCharacter(t *testing.T) {
	short := strings.Repeat("c", 62)
	require.Equal(t, short, truncateIdentifier(short+"é"))
	require.Equal(t, short+"x", truncateIdentifier(short+"xé"))
	mapping := []*protos.TableMapping{{SourceTableIdentifier: "src.t", DestinationTableIdentifier: "dst.t"}}
	schemas := map[string]*protos.TableSchema{
		"src.t": {Columns: []*protos.FieldDescription{{Name: short}, {Name: short + "é"}}},
	}
	require.ErrorContains(t, longIdentifiersError(mapping, schemas, true), "columns dst.t."+short+", dst.t."+short+"é")
}

// Tables added to a running mirror get the key and identifier checks only: not the mirror name's (checked at
// creation) nor the destination tables' (a re-added table's may hold rows; a zero connector has no connection)
func TestValidateAddedTables(t *testing.T) {
	cfg := &protos.FlowConnectionConfigsCore{
		FlowJobName:       strings.Repeat("m", maxIdentifierBytes),
		DoInitialSnapshot: true,
		TableMappings:     []*protos.TableMapping{{SourceTableIdentifier: "src.t", DestinationTableIdentifier: "dst.t"}},
	}
	keyed := map[string]*protos.TableSchema{
		"src.t": {Columns: []*protos.FieldDescription{{Name: "id"}}, PrimaryKeyColumns: []string{"id"}},
	}
	require.NoError(t, (&PostgresConnector{}).ValidateAddedTables(t.Context(), cfg, keyed))
	keyless := map[string]*protos.TableSchema{"src.t": {Columns: []*protos.FieldDescription{{Name: "id"}}}}
	require.ErrorContains(t, (&PostgresConnector{}).ValidateAddedTables(t.Context(), cfg, keyless), "src.t")
	long := map[string]*protos.TableSchema{"src.t": {
		Columns: []*protos.FieldDescription{{Name: "id"}, {Name: strings.Repeat("c", 64)}}, PrimaryKeyColumns: []string{"id"},
	}}
	require.ErrorContains(t, (&PostgresConnector{}).ValidateAddedTables(t.Context(), cfg, long), "column dst.t.")
	// the added tables' column settings are checked too (this one is refused before any lookup on the destination)
	withSetting := proto.CloneOf(cfg)
	withSetting.TableMappings[0].Columns = []*protos.ColumnSetting{{SourceName: "id", DestinationType: "interval"}}
	require.ErrorContains(t, (&PostgresConnector{}).ValidateAddedTables(t.Context(), withSetting, keyed),
		"src.t.id: interval isn't a destination_type")
}
