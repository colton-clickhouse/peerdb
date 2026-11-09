package connpostgres

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// destinationTypeOverrides are the target column types a column setting's destination_type can pick on a Postgres
// destination, per PeerDB type, instead of the type PeerDB creates (qValueKindToPostgresType). Each one holds every
// value of the PeerDB type, takes the snapshot's binary COPY of it, and normalize casts to it (the target's own
// types). Mostly for mappings that can't hold the source's range: unsigned integers, MySQL TIME as a duration.
var destinationTypeOverrides = map[types.QValueKind][]string{
	types.QValueKindInt8:    {"smallint", "integer", "bigint", "numeric"},
	types.QValueKindUInt8:   {"smallint", "integer", "bigint", "numeric"},
	types.QValueKindInt16:   {"integer", "bigint", "numeric"},
	types.QValueKindUInt16:  {"integer", "bigint", "numeric"},
	types.QValueKindInt32:   {"bigint", "numeric"},
	types.QValueKindUInt32:  {"bigint", "numeric"},
	types.QValueKindInt64:   {"numeric"},
	types.QValueKindUInt64:  {"numeric(20,0)", "numeric"},
	types.QValueKindFloat32: {"float8"},
	types.QValueKindTime:    {"interval"},
	types.QValueKindString:  {"citext"},
	types.QValueKindJSON:    {"jsonb"},
	// MySQL TINYINT(1) only, which the MySQL reader then reads as an integer instead of a boolean
	types.QValueKindBoolean: {"smallint"},
}

// GetColumnsTypeConversion lists the destination_type choices for Postgres destinations, for the UI
func GetColumnsTypeConversion() (*protos.ColumnsTypeConversionResponse, error) {
	res := make([]*protos.ColumnsTypeConversion, 0, len(destinationTypeOverrides))
	for qkind, destTypes := range destinationTypeOverrides {
		res = append(res, &protos.ColumnsTypeConversion{Qkind: string(qkind), DestinationTypes: destTypes})
	}
	return &protos.ColumnsTypeConversionResponse{Conversions: res}, nil
}

// columnDestinationTypes returns the destination_type set for each column of the destination table (by source
// column name), from its table mapping
func columnDestinationTypes(tableMappings []*protos.TableMapping, dstTable *common.QualifiedTable) map[string]string {
	for _, tm := range tableMappings {
		parsed, err := common.ParseTableIdentifier(tm.DestinationTableIdentifier)
		if err != nil || parsed.Namespace != dstTable.Namespace || parsed.Table != dstTable.Table {
			continue
		}
		out := make(map[string]string, len(tm.Columns))
		for _, col := range tm.Columns {
			if col.DestinationType != "" {
				out[col.SourceName] = col.DestinationType
			}
		}
		return out
	}
	return nil
}

// destinationTypesError checks the column settings' destination_type of a mirror to Postgres: only on the Q type
// system (the PG type system copies the source's types), only the choices in destinationTypeOverrides for the
// column's PeerDB type, and the type must exist on the destination (citext is an extension). Columns without a
// source schema (resync validation doesn't read it) aren't checked; they were when the table was added.
func (c *PostgresConnector) destinationTypesError(
	ctx context.Context, cfg *protos.FlowConnectionConfigsCore, schemas map[string]*protos.TableSchema,
) error {
	var problems []string
	for _, tm := range cfg.TableMappings {
		schema := schemas[tm.SourceTableIdentifier]
		for _, col := range tm.Columns {
			if col.DestinationType == "" {
				continue
			}
			name := tm.SourceTableIdentifier + "." + col.SourceName
			if cfg.System != protos.TypeSystem_Q {
				problems = append(problems, name+": destination_type needs the Q type system")
				continue
			}
			if schema == nil {
				continue
			}
			idx := slices.IndexFunc(schema.Columns, func(f *protos.FieldDescription) bool { return f.Name == col.SourceName })
			if idx < 0 {
				problems = append(problems, name+": no such column")
				continue
			}
			kind := types.QValueKind(schema.Columns[idx].Type)
			allowed := destinationTypeOverrides[kind]
			if !slices.ContainsFunc(allowed, func(t string) bool { return strings.EqualFold(t, col.DestinationType) }) {
				problems = append(problems, fmt.Sprintf("%s: %s isn't a destination_type for its type %s (choices: %s)",
					name, col.DestinationType, kind, strings.Join(allowed, ", ")))
				continue
			}
			if kind == types.QValueKindBoolean {
				// the MySQL reader reads TINYINT(1) with an integer destination_type as an integer, so a boolean here
				// is another source's, whose values can't be written to an integer column
				problems = append(problems, name+": only MySQL TINYINT(1) columns can be read as integers")
				continue
			}
			var known bool
			if err := c.conn.QueryRow(ctx, `SELECT to_regtype($1) IS NOT NULL`, col.DestinationType).Scan(&known); err != nil {
				return fmt.Errorf("failed to look up type %s on the destination: %w", col.DestinationType, err)
			}
			if !known {
				problems = append(problems, fmt.Sprintf("%s: type %s doesn't exist on the destination", name, col.DestinationType))
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid column type settings for Postgres: %s", strings.Join(problems, "; "))
	}
	return nil
}
