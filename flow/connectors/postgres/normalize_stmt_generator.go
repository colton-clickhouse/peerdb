package connpostgres

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type normalizeStmtGenerator struct {
	// to log fallback statement selection
	log.Logger
	// _PEERDB_RAW_...
	rawTableName string
	// the schema of the table to merge into
	tableSchemaMapping map[string]*protos.TableSchema
	// array of toast column combinations that are unchanged
	unchangedToastColumnsMap map[string][]string
	// _PEERDB_IS_DELETED and _SYNCED_AT columns
	peerdbCols *protos.PeerDBColumns
	// Postgres metadata schema
	metadataSchema string
	// Postgres version 15 introduced MERGE, fallback statements before that
	supportsMerge bool
	// the mirror's internal version
	version uint32
	// per generated MERGE: quoted column -> the select alias telling whether a change carries it, and the literal
	// default an insert without it takes (columns with a literal default)
	presentAs map[string]string
	fillValue map[string]string
	// destination table -> column -> the target column's type without modifiers, read per batch from
	// InternalVersion_PgNormalizeCastToTargetTypes on
	targetColumnTypes map[string]map[string]string
}

func (n *normalizeStmtGenerator) columnTypeToPg(
	dstTableName string, schema *protos.TableSchema, column *protos.FieldDescription,
) string {
	var pgType string
	switch schema.System {
	case protos.TypeSystem_Q:
		// cast to the target column's own type, so columns widened or pre-created by the user hold through
		// CDC as they do for the snapshot COPY; the modifier is left to assignment (an explicit cast to
		// varchar(n) or char(n) would truncate silently)
		if targetType, ok := n.targetColumnTypes[dstTableName][column.Name]; ok {
			pgType = targetType
			kindType := qValueKindToPostgresType(column.Type, n.version)
			if castThroughKindType(types.QValueKind(column.Type), targetType) && !strings.EqualFold(kindType, targetType) {
				pgType = kindType + "::" + targetType
			}
		} else {
			pgType = qValueKindToPostgresType(column.Type, n.version)
		}
	case protos.TypeSystem_PG:
		pgType = column.Type
		// Add schema qualification for user-defined types
		if column.TypeSchemaName != "" {
			schemaQualifiedPgType := common.QualifiedTable{
				Namespace: column.TypeSchemaName,
				Table:     pgType,
			}
			return schemaQualifiedPgType.String()
		}
	default:
		panic(fmt.Sprintf("unsupported system %s", schema.System))
	}

	return pgType
}

// castThroughKindType reports whether a value of kind goes through PeerDB's own Postgres type for it on the way
// to a target column of another type, as by assignment before casts followed the target, instead of being cast
// from its JSON text directly:
//   - floats: read as real or double precision, as the snapshot COPY writes them, then converted (a float's text
//     isn't integer input, and a float32's shortest text isn't the value the snapshot stores in a double column)
//   - numeric into integers: decimal text ('5.00') isn't integer input; numeric rounds
//   - arrays: the elements arrive as text[], which has no cast to most targets (pgvector's vector from REAL[],
//     integer[] from DOUBLE PRECISION[])
//
// Everything else is cast directly, since PeerDB's own type can be narrower than the value: TIME as time can't
// hold -838:59:59, which an interval target can, and BIGINT UNSIGNED as bigint can't hold 2^64-1, which a
// numeric target can.
func castThroughKindType(kind types.QValueKind, targetType string) bool {
	if kind.IsArray() {
		return true
	}
	switch kind {
	case types.QValueKindFloat32, types.QValueKindFloat64:
		return true
	case types.QValueKindNumeric:
		switch targetType {
		case "smallint", "integer", "bigint":
			return true
		}
	}
	return false
}

func (n *normalizeStmtGenerator) generateExpr(
	normalizedTableSchema *protos.TableSchema,
	genericColumnType string,
	stringCol string,
	pgType string,
) string {
	if normalizedTableSchema.System == protos.TypeSystem_Q {
		qkind := types.QValueKind(genericColumnType)
		if qkind.IsArray() {
			return fmt.Sprintf("ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>%s)::JSON))::%s", stringCol, pgType)
		} else if qkind == types.QValueKindBytes {
			return fmt.Sprintf("decode(_peerdb_data->>%s, 'base64')::%s", stringCol, pgType)
		} else if pgType == "bytea" {
			// into a bytea target: text::bytea would read backslash escapes ('\x41' is the byte 'A')
			switch qkind {
			case types.QValueKindString, types.QValueKindEnum, types.QValueKindJSON:
				return fmt.Sprintf("convert_to(_peerdb_data->>%s, 'UTF8')", stringCol)
			case types.QValueKindUUID:
				return fmt.Sprintf("uuid_send((_peerdb_data->>%s)::uuid)", stringCol)
			}
		}
	}
	return fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
}

func (n *normalizeStmtGenerator) generateNormalizeStatements(dstTable string) []string {
	normalizedTableSchema := n.tableSchemaMapping[dstTable]

	if n.supportsMerge {
		unchangedToastColumns := n.unchangedToastColumnsMap[dstTable]
		return []string{n.generateMergeStatement(dstTable, normalizedTableSchema, unchangedToastColumns)}
	}
	n.Warn("Postgres version is not high enough to support MERGE, falling back to UPSERT+DELETE")
	n.Warn("TOAST columns will not be updated properly, use REPLICA IDENTITY FULL or upgrade Postgres")
	if n.peerdbCols.SoftDeleteColName != "" {
		n.Warn("soft delete enabled with fallback statements! this combination is unsupported")
	}
	return n.generateFallbackStatements(dstTable, normalizedTableSchema)
}

func (n *normalizeStmtGenerator) generateFallbackStatements(
	dstTableName string,
	normalizedTableSchema *protos.TableSchema,
) []string {
	columnCount := len(normalizedTableSchema.Columns)
	columnNames := make([]string, 0, columnCount)
	flattenedCastsSQLArray := make([]string, 0, columnCount)
	primaryKeyColumnCasts := make(map[string]string, len(normalizedTableSchema.PrimaryKeyColumns))
	for _, column := range normalizedTableSchema.Columns {
		genericColumnType := column.Type
		quotedCol := common.QuoteIdentifier(column.Name)
		stringCol := utils.QuoteLiteral(column.Name)
		columnNames = append(columnNames, quotedCol)
		pgType := n.columnTypeToPg(dstTableName, normalizedTableSchema, column)
		expr := n.generateExpr(normalizedTableSchema, genericColumnType, stringCol, pgType)

		flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("%s AS %s", expr, quotedCol))
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			primaryKeyColumnCasts[column.Name] = expr
		}
	}
	flattenedCastsSQL := strings.Join(flattenedCastsSQLArray, ",")
	parsedDstTable, _ := common.ParseTableIdentifier(dstTableName)

	insertColumnsSQL := strings.Join(columnNames, ",")
	updateColumnsSQLArray := make([]string, 0, columnCount)
	for _, column := range normalizedTableSchema.Columns {
		quotedCol := common.QuoteIdentifier(column.Name)
		updateColumnsSQLArray = append(updateColumnsSQLArray, fmt.Sprintf(`%s=EXCLUDED.%s`, quotedCol, quotedCol))
	}
	updateColumnsSQL := strings.Join(updateColumnsSQLArray, ",")
	deleteWhereClauseArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	for columnName, columnCast := range primaryKeyColumnCasts {
		deleteWhereClauseArray = append(deleteWhereClauseArray, fmt.Sprintf(`%s.%s=%s`,
			parsedDstTable.String(), common.QuoteIdentifier(columnName), columnCast))
	}
	deleteWhereClauseSQL := strings.Join(deleteWhereClauseArray, " AND ")

	// make it update instead in case soft-delete is enabled
	deleteUpdate := fmt.Sprintf(`DELETE FROM %s USING `, parsedDstTable.String())
	if n.peerdbCols.SoftDeleteColName != "" {
		deleteUpdate = fmt.Sprintf(`UPDATE %s SET %s=TRUE`,
			parsedDstTable.String(), common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName))
		if n.peerdbCols.SyncedAtColName != "" {
			deleteUpdate += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		}
		deleteUpdate += " FROM"
	}
	fallbackUpsertStatement := fmt.Sprintf(fallbackUpsertStatementSQL,
		strings.Join(slices.Collect(maps.Values(primaryKeyColumnCasts)), ","), n.metadataSchema,
		n.rawTableName, parsedDstTable.String(), insertColumnsSQL, flattenedCastsSQL,
		strings.Join(normalizedTableSchema.PrimaryKeyColumns, ","), updateColumnsSQL)
	fallbackDeleteStatement := fmt.Sprintf(fallbackDeleteStatementSQL,
		strings.Join(slices.Collect(maps.Values(primaryKeyColumnCasts)), ","), n.metadataSchema,
		n.rawTableName, deleteUpdate, deleteWhereClauseSQL)

	return []string{fallbackUpsertStatement, fallbackDeleteStatement}
}

func (n *normalizeStmtGenerator) generateMergeStatement(
	dstTableName string,
	normalizedTableSchema *protos.TableSchema,
	unchangedToastColumns []string,
) string {
	columnCount := len(normalizedTableSchema.Columns)
	quotedColumnNames := make([]string, columnCount)

	parsedDstTable, _ := common.ParseTableIdentifier(dstTableName)

	primaryKeyColumnCasts := make(map[string]string, len(normalizedTableSchema.PrimaryKeyColumns))
	primaryKeySelectSQLArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))

	// For jsonb_to_record path we build:
	//   recordDefs  – column definitions for the AS clause of jsonb_to_record (in the CTE)
	//   selectExprs – the SELECT list in the USING subquery, referencing columns from src_rank.
	//                 json/jsonb columns are wrapped with _peerdb_parse_jsonb/_peerdb_parse_json
	//                 to unwrap PeerDB's stringified representation.
	// For legacy path we only build selectExprs (flattened casts via ->>).
	selectExprs := make([]string, 0, columnCount)
	recordDefs := make([]string, 0, columnCount)
	useJsonbToRecord := normalizedTableSchema.System == protos.TypeSystem_PG
	// columns with a literal default (from the source, e.g. added mid-stream with ADD COLUMN ... DEFAULT): a change
	// synced before the column existed doesn't carry it, so an insert takes the default and an update keeps the
	// row's value, as on the source, instead of writing NULL
	n.presentAs, n.fillValue = map[string]string{}, map[string]string{}
	for i, column := range normalizedTableSchema.Columns {
		quotedCol := common.QuoteIdentifier(column.Name)
		stringCol := utils.QuoteLiteral(column.Name)
		quotedColumnNames[i] = quotedCol
		pgType := n.columnTypeToPg(dstTableName, normalizedTableSchema, column)

		if useJsonbToRecord {
			// json/jsonb columns are stored as stringified text inside _peerdb_data,
			// so jsonb_to_record extracts them as a jsonb string wrapper.
			// Include them in the record as jsonb, then unwrap in the USING SELECT.
			switch column.Type {
			case "json":
				recordDefs = append(recordDefs, quotedCol+" jsonb")
				selectExprs = append(selectExprs, fmt.Sprintf("(%s #>> '{}')::json AS %s", quotedCol, quotedCol))
			case "jsonb":
				recordDefs = append(recordDefs, quotedCol+" jsonb")
				selectExprs = append(selectExprs, fmt.Sprintf("(%s #>> '{}')::jsonb AS %s", quotedCol, quotedCol))
			default:
				recordDefs = append(recordDefs, fmt.Sprintf("%s %s", quotedCol, pgType))
				selectExprs = append(selectExprs, quotedCol)
			}
		} else {
			genericColumnType := column.Type
			expr := n.generateExpr(normalizedTableSchema, genericColumnType, stringCol, pgType)
			selectExprs = append(selectExprs, fmt.Sprintf("%s AS %s", expr, quotedCol))
			if fill, ok := literalDefault(column, pgType); ok {
				alias := fmt.Sprintf(`"_peerdb_has_%d"`, i)
				selectExprs = append(selectExprs, fmt.Sprintf("(_peerdb_data ? %s) AS %s", stringCol, alias))
				n.presentAs[quotedCol], n.fillValue[quotedCol] = alias, fill
			}
		}

		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			if !useJsonbToRecord {
				primaryKeyColumnCasts[column.Name] = fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
			}
			primaryKeySelectSQLArray = append(primaryKeySelectSQLArray, fmt.Sprintf("src.%s=dst.%s", quotedCol, quotedCol))
		}
	}
	selectExprsSQL := strings.Join(selectExprs, ",")
	insertValuesSQLArray := make([]string, 0, columnCount+2)
	for _, quotedCol := range quotedColumnNames {
		if alias, ok := n.presentAs[quotedCol]; ok {
			insertValuesSQLArray = append(insertValuesSQLArray,
				fmt.Sprintf("CASE WHEN src.%s THEN src.%s ELSE %s END", alias, quotedCol, n.fillValue[quotedCol]))
		} else {
			insertValuesSQLArray = append(insertValuesSQLArray, "src."+quotedCol)
		}
	}

	updateStatementsforToastCols := n.generateUpdateStatements(quotedColumnNames, unchangedToastColumns)
	// append synced_at column
	if n.peerdbCols.SyncedAtColName != "" {
		quotedColumnNames = append(quotedColumnNames, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		insertValuesSQLArray = append(insertValuesSQLArray, "CURRENT_TIMESTAMP")
	}
	insertColumnsSQL := strings.Join(quotedColumnNames, ",")
	insertValuesSQL := strings.Join(insertValuesSQLArray, ",")

	if n.peerdbCols.SoftDeleteColName != "" {
		softDeleteInsertColumnsSQL := strings.Join(
			append(quotedColumnNames, common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName)), ",")
		softDeleteInsertValuesSQL := strings.Join(append(insertValuesSQLArray, "TRUE"), ",")

		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND (src._peerdb_record_type=2) THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, "\n")

	conflictPart := "DELETE"
	if n.peerdbCols.SoftDeleteColName != "" {
		colName := n.peerdbCols.SoftDeleteColName
		conflictPart = fmt.Sprintf(`UPDATE SET %s=TRUE`, common.QuoteIdentifier(colName))
		if n.peerdbCols.SyncedAtColName != "" {
			conflictPart += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		}
	}

	var mergeStmt string
	if useJsonbToRecord {
		// PARTITION BY uses quoted PK column names directly — jsonb_to_record
		// already extracted them in the CTE via r.*
		primaryKeyQuotedNames := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
		for _, pkCol := range normalizedTableSchema.PrimaryKeyColumns {
			primaryKeyQuotedNames = append(primaryKeyQuotedNames, common.QuoteIdentifier(pkCol))
		}
		mergeStmt = fmt.Sprintf(
			mergeStatementSQLJsonbToRecord,
			strings.Join(primaryKeyQuotedNames, ","),
			n.metadataSchema,
			n.rawTableName,
			strings.Join(recordDefs, ","),
			parsedDstTable.String(),
			selectExprsSQL,
			strings.Join(primaryKeySelectSQLArray, " AND "),
			insertColumnsSQL,
			insertValuesSQL,
			updateStringToastCols,
			conflictPart,
		)
	} else {
		mergeStmt = fmt.Sprintf(
			mergeStatementSQL,
			strings.Join(slices.Collect(maps.Values(primaryKeyColumnCasts)), ","),
			n.metadataSchema,
			n.rawTableName,
			parsedDstTable.String(),
			selectExprsSQL,
			strings.Join(primaryKeySelectSQLArray, " AND "),
			insertColumnsSQL,
			insertValuesSQL,
			updateStringToastCols,
			conflictPart,
		)
	}

	return mergeStmt
}

func (n *normalizeStmtGenerator) generateUpdateStatements(quotedCols []string, unchangedToastColumns []string) []string {
	handleSoftDelete := n.peerdbCols.SoftDeleteColName != ""
	stmtCount := len(unchangedToastColumns)
	if handleSoftDelete {
		stmtCount *= 2
	}
	updateStmts := make([]string, 0, stmtCount)

	for _, cols := range unchangedToastColumns {
		unchangedColsArray := strings.Split(cols, ",")
		for i, unchangedToastCol := range unchangedColsArray {
			unchangedColsArray[i] = common.QuoteIdentifier(unchangedToastCol)
		}
		otherCols := shared.ArrayMinus(quotedCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols))
		for _, colName := range otherCols {
			if alias, ok := n.presentAs[colName]; ok {
				tmpArray = append(tmpArray, fmt.Sprintf("%s=CASE WHEN src.%s THEN src.%s ELSE dst.%s END",
					colName, alias, colName, colName))
			} else {
				tmpArray = append(tmpArray, fmt.Sprintf("%s=src.%s", colName, colName))
			}
		}
		// set the synced at column to the current timestamp
		if n.peerdbCols.SyncedAtColName != "" {
			tmpArray = append(tmpArray, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName)+`=CURRENT_TIMESTAMP`)
		}
		// set soft-deleted to false, tackles insert after soft-delete
		if handleSoftDelete {
			tmpArray = append(tmpArray, common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName)+`=FALSE`)
		}

		quotedCols := utils.QuoteLiteral(cols)
		ssep := strings.Join(tmpArray, ",")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=%s
			THEN UPDATE SET %s`, quotedCols, ssep)
		updateStmts = append(updateStmts, updateStmt)

		// generates update statements for the case where updates and deletes happen in the same branch
		// the backfill has happened from the pull side already, so treat the DeleteRecord as an update
		// and then set soft-delete to true.
		if handleSoftDelete {
			tmpArray[len(tmpArray)-1] = common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName) + `=TRUE`
			ssep := strings.Join(tmpArray, ", ")
			updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns=%s
			THEN UPDATE SET %s`, quotedCols, ssep)
			updateStmts = append(updateStmts, updateStmt)
		}
	}
	return updateStmts
}

// literalDefault is a Q-system column's default as a Postgres literal, if it has one that isn't a function of the
// time (a CURRENT_TIMESTAMP evaluated at normalize would be a wrong value, not the source's)
func literalDefault(column *protos.FieldDescription, pgType string) (string, bool) {
	if column.DefaultExpr == nil || *column.DefaultExpr == "CURRENT_TIMESTAMP" {
		return "", false
	}
	expr, ok := pgDefaultExpr(*column.DefaultExpr, types.QValueKind(column.Type), pgType)
	if !ok || strings.Contains(strings.ToUpper(expr), "CURRENT_TIMESTAMP") {
		return "", false
	}
	return expr, true
}
