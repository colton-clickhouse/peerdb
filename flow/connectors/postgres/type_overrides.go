package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// destinationTypeOverrides are the target column types a column setting's destination_type can pick on a Postgres
// destination, per PeerDB type, instead of the type PeerDB creates (qValueKindToPostgresType). Each one holds every
// value of the PeerDB type, takes the snapshot's binary COPY of it, and normalize casts to it (the target's own
// types). Only for mappings that can't hold the source's range or meaning: wider integers for unsigned ones, MySQL
// TIME as a duration, INTERVAL as an interval, TINYINT(1) as an integer. Other target
// types are the user's to pre-create.
var destinationTypeOverrides = map[types.QValueKind][]string{
	types.QValueKindInt8:   {"smallint", "integer", "bigint", "numeric"},
	types.QValueKindUInt8:  {"smallint", "integer", "bigint", "numeric"},
	types.QValueKindInt16:  {"integer", "bigint", "numeric"},
	types.QValueKindUInt16: {"integer", "bigint", "numeric"},
	types.QValueKindInt32:  {"bigint", "numeric"},
	types.QValueKindUInt32: {"bigint", "numeric"},
	types.QValueKindInt64:  {"numeric"},
	types.QValueKindUInt64: {"numeric(20,0)", "numeric"},
	types.QValueKindTime:   {"interval"},
	// INTERVAL (CockroachDB, Postgres sources on the Q type system), which PeerDB creates as text holding its interval
	// JSON: such a column gets the intervals themselves (intervalColumns)
	types.QValueKindInterval:      {"interval"},
	types.QValueKindArrayInterval: {"interval[]"},
	// MySQL TINYINT(1) only, which the MySQL reader then reads as an integer instead of a boolean
	types.QValueKindBoolean: {"smallint"},
}

// intervalColumns are the columns whose destination_type is interval or interval[]: their intervals are written as
// intervals, on both paths, where other columns keep PeerDB's interval JSON
func intervalColumns(columns []*protos.ColumnSetting) map[string]struct{} {
	var out map[string]struct{}
	for _, col := range columns {
		if t := strings.ToLower(col.DestinationType); t == "interval" || t == "interval[]" {
			if out == nil {
				out = make(map[string]struct{})
			}
			out[col.SourceName] = struct{}{}
		}
	}
	return out
}

// GetColumnsTypeConversion lists the destination_type choices for Postgres destinations, for the API (the mirror
// creation UI offers column types for ClickHouse destinations only)
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

// destinationTypeProblem says why destinationType can't be the type of a column of PeerDB type kind, or "" if it can:
// only on the Q type system (the PG type system copies the source's types), only the choices in
// destinationTypeOverrides for the column's type, and a boolean only as the MySQL reader leaves it: it reads TINYINT(1)
// with an integer destination_type as an integer, so a boolean here is another source's (or TINYINT(1) read without the
// destination's type), whose values can't be written to an integer column.
func destinationTypeProblem(system protos.TypeSystem, kind types.QValueKind, destinationType string) string {
	if system != protos.TypeSystem_Q {
		return "destination_type needs the Q type system"
	}
	allowed := destinationTypeOverrides[kind]
	if !slices.ContainsFunc(allowed, func(t string) bool { return strings.EqualFold(t, destinationType) }) {
		return fmt.Sprintf("%s isn't a destination_type for its type %s (choices: %s)",
			destinationType, kind, strings.Join(allowed, ", "))
	}
	if kind == types.QValueKindBoolean {
		return "only MySQL TINYINT(1) columns can be read as integers"
	}
	return ""
}

// destinationTypesError checks the column settings' destination_type of a mirror to Postgres (destinationTypeProblem),
// and that the type is the destination's built-in one. Columns without a source schema aren't checked against their
// type: table creation checks them again (checkDestinationTypes).
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
				problems = append(problems, name+": "+destinationTypeProblem(cfg.System, "", col.DestinationType))
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
			if problem := destinationTypeProblem(cfg.System, types.QValueKind(schema.Columns[idx].Type), col.DestinationType); problem != "" {
				problems = append(problems, name+": "+problem)
				continue
			}
			// CREATE TABLE names the type unqualified, so it resolves it with search_path, as to_regtype does here: on a
			// connection of the same peer (same role and database, PeerDB doesn't set search_path). pg_catalog comes
			// first unless search_path names it later, so this also refuses a type of the same name that comes first,
			// which needn't take the snapshot's COPY or normalize's cast.
			var builtin *bool
			if err := c.conn.QueryRow(ctx,
				`SELECT typnamespace = 'pg_catalog'::regnamespace FROM pg_type WHERE oid = to_regtype($1)`,
				col.DestinationType,
			).Scan(&builtin); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("failed to look up type %s on the destination: %w", col.DestinationType, err)
			}
			if builtin == nil {
				problems = append(problems, fmt.Sprintf("%s: type %s doesn't exist on the destination", name, col.DestinationType))
			} else if !*builtin {
				problems = append(problems, fmt.Sprintf("%s: type %s resolves to another schema's type than the built-in one",
					name, col.DestinationType))
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid column type settings for Postgres: %s", strings.Join(problems, "; "))
	}
	return nil
}

// checkDestinationTypes refuses, at table creation, a destination_type its column can't have (destinationTypeProblem):
// validation is skipped with skip_validation, and a setting validation didn't see would otherwise create a table the
// snapshot or normalize can't write to, or one that changes values.
func checkDestinationTypes(
	tableSchema *protos.TableSchema, destinationTypes map[string]string, dstTable *common.QualifiedTable,
) error {
	var problems []string
	for _, column := range tableSchema.Columns {
		if destinationType, ok := destinationTypes[column.Name]; ok {
			if problem := destinationTypeProblem(tableSchema.System, types.QValueKind(column.Type), destinationType); problem != "" {
				problems = append(problems, column.Name+": "+problem)
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid column type settings for Postgres table %s: %s", dstTable, strings.Join(problems, "; "))
	}
	return nil
}
