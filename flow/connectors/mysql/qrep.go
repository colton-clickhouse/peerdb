package connmysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MySqlConnector) tableRowEstimate(ctx context.Context, schema string, table string) (int64, error) {
	rs, err := c.Execute(ctx, fmt.Sprintf("select table_rows from information_schema.tables where table_schema='%s' and table_name='%s'",
		mysql.Escape(schema), mysql.Escape(table)))
	if err != nil {
		return 0, fmt.Errorf("failed to query information schema for row count estimate: %w", err)
	}
	defer rs.Close()
	return rs.GetInt(0, 0)
}

func (c *MySqlConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
		// if no watermark column is specified, return a single partition
		return utils.FullTablePartition(), nil
	}

	if config.NumPartitionsOverride == 0 && config.NumRowsPerPartition == 0 {
		return nil, fmt.Errorf("num rows per partition must be greater than 0")
	}

	numPartitions := int64(config.NumPartitionsOverride)
	numRowsPerPartition := int64(config.NumRowsPerPartition)

	parsedWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}

	minmaxQuery := fmt.Sprintf("SELECT MIN(`%[2]s`),MAX(`%[2]s`) FROM %[1]s",
		parsedWatermarkTable.MySQL(), config.WatermarkColumn)
	var minmaxHasCount bool
	if last != nil && last.Range != nil && numPartitions == 0 {
		// we resume replication from the last partition, we need to include count of the remaining rows in the query
		minmaxHasCount = true
		minmaxQuery = fmt.Sprintf("SELECT MIN(`%[2]s`),MAX(`%[2]s`),COUNT(*) FROM %[1]s",
			parsedWatermarkTable.MySQL(), config.WatermarkColumn)
	} else if numPartitions == 0 {
		// we are starting replication from the beginning and need to estimate approximate rows count to calculate partitions
		totalRows, err := c.tableRowEstimate(ctx, parsedWatermarkTable.Namespace, parsedWatermarkTable.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}

		if totalRows == 0 {
			c.logger.Warn("estimating no records to replicate, only using 1 partition")
			numPartitions = 1
		} else {
			// Calculate the number of partitions
			adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
			c.logger.Info("[mysql] partition details",
				slog.Int64("totalRowsEstimate", totalRows),
				slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
				slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
				slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))

			numPartitions = adjustedPartitions.AdjustedNumPartitions
		}
	}

	var rs *mysql.Result
	if last != nil && last.Range != nil {
		var minVal string
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = strconv.FormatInt(lastRange.IntRange.End, 10)
		case *protos.PartitionRange_UintRange:
			minVal = strconv.FormatUint(lastRange.UintRange.End, 10)
		case *protos.PartitionRange_TimestampRange:
			time := lastRange.TimestampRange.End.AsTime()
			minVal = "'" + time.Format("2006-01-02 15:04:05.999999") + "'"
		case *protos.PartitionRange_StringRange:
			// resuming from last partition range is only possible for standalone QRepFlowWorkflow;
			// this is a legacy feature and string partitioning is not supported
			return nil, errors.New("resuming QRep by a string partition range is not supported")
		case *protos.PartitionRange_NullRange:
			// this case should never happen because we only add null partition for InitialCopyOnly replication
			// (so there shouldn't be a resume scenario with null range)
			return nil, errors.New("unexpected null range in last partition after resuming QRep")
		}

		minmaxQuery = fmt.Sprintf("%s WHERE `%s` > %s", minmaxQuery, config.WatermarkColumn, minVal)
	}
	c.logger.Info("querying min/max", slog.String("query", minmaxQuery))
	rs, err = c.Execute(ctx, minmaxQuery)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	if minmaxHasCount {
		totalRows, err := rs.GetInt(0, 2)
		if err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}

		if totalRows == 0 {
			c.logger.Warn("no records to replicate, returning")
			return make([]*protos.QRepPartition, 0), nil
		}

		// Calculate the number of partitions
		adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
		c.logger.Info("[mysql] partition details",
			slog.Int64("totalRows", totalRows),
			slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
			slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
			slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))

		numPartitions = adjustedPartitions.AdjustedNumPartitions
	}

	watermarkField := rs.Fields[1]
	watermarkMyType := watermarkField.Type
	watermarkUnsigned := (watermarkField.Flag & mysql.UNSIGNED_FLAG) != 0
	watermarkQKind, err := qkindFromMysqlType(watermarkField.Type, watermarkUnsigned, watermarkField.Charset, config.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to convert mysql type to qvaluekind: %w", err)
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	val1, err := QValueFromMysqlFieldValue(watermarkQKind, watermarkMyType, rs.Values[0][0], config.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to convert partition minimum to qvalue: %w", err)
	}
	val2, err := QValueFromMysqlFieldValue(watermarkQKind, watermarkMyType, rs.Values[0][1], config.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to convert partition maximum to qvalue: %w", err)
	}

	if watermarkQKind == types.QValueKindDate || watermarkQKind == types.QValueKindTimestamp {
		minFV, maxFV := rs.Values[0][0], rs.Values[0][1]
		if minFV.Type == mysql.FieldValueTypeString && maxFV.Type == mysql.FieldValueTypeString {
			return c.temporalPartitions(ctx, config, last, parsedWatermarkTable,
				string(minFV.AsString()), string(maxFV.AsString()), numPartitions)
		}
	}

	start, startIsString := val1.Value().(string)
	end, endIsString := val2.Value().(string)
	if startIsString && endIsString {
		if isUUID, casing := detectUuidWithHexCasing(start, end); isUUID {
			uuidPartitions, err := buildUuidStringPartitions(start, end, casing, numPartitions)
			if err != nil {
				return nil, fmt.Errorf("failed to add uuid string partitions: %w", err)
			}
			partitionHelper.AddPartitions(uuidPartitions)
		} else {
			stringPartitions, err := buildAdaptiveStringPartitions(
				ctx, c, c.logger, parsedWatermarkTable, config.WatermarkColumn, start, end, numPartitions)
			if err != nil {
				return nil, fmt.Errorf("failed to build adaptive string partitions: %w", err)
			}
			partitionHelper.AddPartitions(stringPartitions)
		}
	} else if err := partitionHelper.AddPartitionsWithRange(val1.Value(), val2.Value(), numPartitions); err != nil {
		return nil, fmt.Errorf("failed to add partitions: %w", err)
	}

	// add null values partition to the end, if nulls aren't present it will be an empty partition
	// that gets skipped during replication
	if config.AddNullPartition {
		partitionHelper.AddNullPartition()
	}

	return partitionHelper.GetPartitions(), nil
}

// temporalPartitions partitions a DATE/DATETIME watermark by bounds read as MySQL strings, not as converted values:
// a zero date, and by version any zero part, converts to the epoch, so a MAX like '2025-03-00' ended the range
// before its start and a zero MIN skipped every row before 1970.
//   - A MIN with a zero month or day (year >= 1) is floored to the last real date below it, and a MAX is rounded up
//     to the first real date above it ('2025-03-00' sorts between '2025-02-28' and '2025-03-01').
//   - Values with year 0 sort below every real date, which a timestamp range can't start at. A snapshot reads them
//     in one extra partition, '0000-00-00' <= v < '0001-01-01', and range partitions the rest from the first real
//     date. Incremental QRep can't: its next cycle would resume from that partition, so it refuses the table.
//   - Resuming, the range starts after the last partition's end even if the floor is below it.
//
// Zero-part values inside the range stay in the partition MySQL sorts them into, since partitions are read as
// v >= start AND v < next start (pullTemporalRange).
func (c *MySqlConnector) temporalPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
	table *common.QualifiedTable,
	minStr, maxStr string,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	var partitions []*protos.QRepPartition
	if isZeroYear(minStr) {
		if config.Query != "" || !config.InitialCopyOnly {
			return nil, fmt.Errorf("watermark column %s of %s has dates in year 0 (minimum %q), which an incremental "+
				"QRep partition can't start at; use another watermark column", config.WatermarkColumn, config.WatermarkTable, minStr)
		}
		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{Range: &protos.PartitionRange_StringRange{
				StringRange: &protos.StringPartitionRange{Start: "0000-00-00 00:00:00", End: "0001-01-01 00:00:00"},
			}},
		})
		rs, err := c.Execute(ctx, fmt.Sprintf("SELECT MIN(%[2]s) FROM %[1]s WHERE %[2]s >= '0001-01-01'",
			table.MySQL(), common.QuoteMySQLIdentifier(config.WatermarkColumn)))
		if err != nil {
			return nil, fmt.Errorf("failed to query the first real date of %s: %w", config.WatermarkColumn, err)
		}
		realMin := rs.Values[0][0]
		rs.Close()
		if realMin.Type != mysql.FieldValueTypeString {
			minStr = "" // only year 0
		} else {
			minStr = string(realMin.AsString())
		}
	}
	if minStr != "" && !isZeroYear(maxStr) {
		start, err := floorMysqlDateTime(minStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition minimum %q: %w", minStr, err)
		}
		if last != nil && last.Range != nil {
			if r, ok := last.Range.Range.(*protos.PartitionRange_TimestampRange); ok {
				if after := r.TimestampRange.End.AsTime().Add(time.Microsecond); start.Before(after) {
					start = after
				}
			}
		}
		end, err := ceilMysqlDateTime(maxStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition maximum %q: %w", maxStr, err)
		}
		partitionHelper := utils.NewPartitionHelper(c.logger)
		if err := partitionHelper.AddPartitionsWithRange(start, end, numPartitions); err != nil {
			return nil, fmt.Errorf("failed to add partitions: %w", err)
		}
		partitions = append(partitions, partitionHelper.GetPartitions()...)
	}
	if config.AddNullPartition {
		partitionHelper := utils.NewPartitionHelper(c.logger)
		partitionHelper.AddNullPartition()
		partitions = append(partitions, partitionHelper.GetPartitions()...)
	}
	return partitions, nil
}

// timestampRangeQuery reads a timestamp partition as start <= v < end + 1µs instead of BETWEEN start AND end. The
// next partition starts at end + 1µs, and MySQL sorts a zero day ('2025-03-00 12:00:00') between the last
// microsecond of February and March 1st, so BETWEEN misses it when a boundary falls there.
func timestampRangeQuery(selectedColumns, table, column string, end time.Time) (string, string) {
	return fmt.Sprintf("SELECT %[1]s FROM %[2]s WHERE %[3]s >= {{.start}} AND %[3]s < {{.end}}", selectedColumns, table, column),
		"'" + end.Add(time.Microsecond).Format("2006-01-02 15:04:05.999999") + "'"
}

// isZeroYear reports a date in year 0, which sorts below every real date
func isZeroYear(s string) bool {
	return strings.HasPrefix(s, "0000")
}

// floorMysqlDateTime returns the latest real date at or below s: s itself, or for a zero month or day the day
// before the first real date above it ('2025-03-00' -> '2025-02-28', '2025-00-10' -> '2024-12-31')
func floorMysqlDateTime(s string) (time.Time, error) {
	if !hasZeroDatePart(s) {
		if past, lastDay := pastMonthEnd(s); past {
			return lastDay, nil
		}
		return parseMysqlDateTime(s)
	}
	t, err := time.Parse(time.DateOnly, rollZeroDatePartsForward(s[:10]))
	if err != nil {
		return time.Time{}, err
	}
	if s[5:7] == "00" {
		t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	} else {
		t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return t.AddDate(0, 0, -1), nil
}

// ceilMysqlDateTime returns the earliest real date at or above s: a zero month or day becomes 01
// ('2025-03-00' -> '2025-03-01'), and a day past the month's end (ALLOW_INVALID_DATES: '2025-02-30') the next
// month's first day
func ceilMysqlDateTime(s string) (time.Time, error) {
	s = rollZeroDatePartsForward(s)
	if past, lastDay := pastMonthEnd(s); past {
		return lastDay.AddDate(0, 0, 1), nil
	}
	return parseMysqlDateTime(s)
}

// pastMonthEnd reports a day past the end of its month, which sql_mode ALLOW_INVALID_DATES permits
// ('2025-02-30'), with the month's last day
func pastMonthEnd(s string) (bool, time.Time) {
	if len(s) < 10 {
		return false, time.Time{}
	}
	y, errY := strconv.Atoi(s[0:4])
	m, errM := strconv.Atoi(s[5:7])
	d, errD := strconv.Atoi(s[8:10])
	if errY != nil || errM != nil || errD != nil || m < 1 || m > 12 {
		return false, time.Time{}
	}
	lastDay := time.Date(y, time.Month(m)+1, 0, 0, 0, 0, 0, time.UTC)
	return d > lastDay.Day(), lastDay
}

// hasZeroDatePart reports a zero month or day
func hasZeroDatePart(s string) bool {
	return len(s) >= 10 && (s[5:7] == "00" || s[8:10] == "00")
}

// rollZeroDatePartsForward turns a zero month or day into 01, which sorts after it
func rollZeroDatePartsForward(s string) string {
	if !hasZeroDatePart(s) {
		return s
	}
	b := []byte(s)
	if s[5:7] == "00" {
		b[6] = '1'
	}
	if s[8:10] == "00" {
		b[9] = '1'
	}
	return string(b)
}

func parseMysqlDateTime(s string) (time.Time, error) {
	if len(s) == len(time.DateOnly) {
		return time.Parse(time.DateOnly, s)
	}
	return time.Parse("2006-01-02 15:04:05.999999", s)
}

func supportsRangePartition(qkind types.QValueKind) bool {
	switch qkind {
	// integer types
	case types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32, types.QValueKindUInt64:
		return true
	// temporal types
	case types.QValueKindDate, types.QValueKindTimestamp:
		return true
	// string types
	case types.QValueKindString:
		return true
	default:
		return false
	}
}

func (c *MySqlConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	c.logger.Info("Evaluating if tables can perform parallel load")

	output := &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: make(map[string]string, len(input.TableMappings)),
	}
	for _, tm := range input.TableMappings {
		source := tm.SourceTableIdentifier
		schema, ok := input.TableSchemaMapping[source]
		if !ok {
			c.logger.Warn("[mysql] table schema not found, defaulting to full table snapshot",
				slog.String("table", source))
			continue
		}
		if len(schema.PrimaryKeyColumns) == 0 {
			c.logger.Info("[mysql] table has no primary key, defaulting to full table snapshot",
				slog.String("table", source))
			continue
		}
		pkColumn := schema.PrimaryKeyColumns[0]
		var pkQKind types.QValueKind
		for _, col := range schema.Columns {
			if col.Name == pkColumn {
				pkQKind = types.QValueKind(col.Type)
				break
			}
		}
		if !supportsRangePartition(pkQKind) {
			c.logger.Info("[mysql] primary key type does not support range partitioning, defaulting to full table snapshot",
				slog.String("table", source),
				slog.String("column", pkColumn),
				slog.String("qkind", string(pkQKind)))
			continue
		}
		c.logger.Info("[mysql] using primary key as default partition key",
			slog.String("table", source),
			slog.String("column", pkColumn),
			slog.String("qkind", string(pkQKind)))
		output.TableDefaultPartitionKeyMapping[source] = pkColumn
	}
	return output, nil
}

func buildSelectedColumns(cols []*protos.FieldDescription, exclude []string) string {
	columns := make([]string, 0, len(cols))
	for _, col := range cols {
		if slices.Contains(exclude, col.Name) {
			continue
		}

		converted := common.QuoteMySQLIdentifier(col.Name)
		if col.Type == string(types.QValueKindUint16Enum) || col.Type == string(types.QValueKindUint64Set) {
			converted = fmt.Sprintf("CAST(%s AS UNSIGNED) AS %s", converted, converted)
		}
		columns = append(columns, converted)
	}

	return strings.Join(columns, ", ")
}

func (c *MySqlConnector) PullQRepRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	tableSchema, err := c.getTableSchemaForTable(ctx, config.Env,
		// with the column settings, which can pick a column's decoding (TINYINT(1)), as when the table was set up
		&protos.TableMapping{SourceTableIdentifier: config.WatermarkTable, Columns: config.Columns}, protos.TypeSystem_Q,
		config.Version)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get schema for watermark table %s: %w", config.WatermarkTable, err)
	}

	selectedColumns := buildSelectedColumns(tableSchema.Columns, config.Exclude)
	if selectedColumns == "" {
		return 0, 0, fmt.Errorf("no columns selected for watermark table %s (check Exclude configuration)", config.WatermarkTable)
	}

	parsedSrcTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		c.logger.Error("unable to parse source table", slog.Any("error", err))
		return 0, 0, fmt.Errorf("unable to parse source table: %w", err)
	}

	c.logger.Info("[mysql] pulling records start")

	c.totalBytesRead.Store(0)
	c.deltaBytesRead.Store(0)
	totalRecords := int64(0)
	onResult := func(rs *mysql.Result) error {
		schema, err := QRecordSchemaFromMysqlFields(tableSchema, rs.Fields, config.Version)
		if err != nil {
			return err
		}
		stream.SetSchema(schema)
		return nil
	}
	var rs mysql.Result
	onRow := func(row []mysql.FieldValue) error {
		totalRecords += 1
		schema, err := stream.Schema()
		if err != nil {
			return err
		}
		record := make([]types.QValue, 0, len(row))
		for idx, val := range row {
			if rs.Fields[idx].Flag&mysql.PRI_KEY_FLAG != 0 && config.Version >= shared.InternalVersion_MySQLZeroDatePartsAsZero &&
				(schema.Fields[idx].Type == types.QValueKindDate || schema.Fields[idx].Type == types.QValueKindTimestamp) &&
				val.Type == mysql.FieldValueTypeString && isZeroDate(string(val.AsString()), config.Version) {
				return exceptions.NewMySQLZeroDateKeyError(config.WatermarkTable, schema.Fields[idx].Name, string(val.AsString()))
			}
			qv, err := QValueFromMysqlFieldValue(schema.Fields[idx].Type, rs.Fields[idx].Type, val, config.Version)
			if err != nil {
				return fmt.Errorf("could not convert mysql value for %s: %w", schema.Fields[idx].Name, err)
			}
			record = append(record, notNullBinary(qv, rs.Fields[idx].Flag&mysql.NOT_NULL_FLAG != 0))
		}

		if err := stream.Send(ctx, record); err != nil {
			return fmt.Errorf("failed to send record to stream: %w", err)
		}

		if totalRecords%50000 == 0 {
			c.logger.Info("[mysql] pulling records",
				slog.Int64("records", totalRecords),
				slog.Int64("bytes", c.totalBytesRead.Load()),
				slog.Int("channelLen", len(stream.Records)))
		}

		return nil
	}

	shutDown := common.Interval(ctx, time.Minute, func() {
		read := c.deltaBytesRead.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
	})
	defer shutDown()

	if partition.FullTablePartition {
		query := config.Query
		if query == "" {
			query = fmt.Sprintf("SELECT %s FROM %s", selectedColumns, parsedSrcTable.MySQL())
		}

		if err := c.ExecuteSelectStreaming(ctx, query, &rs, onRow, onResult); err != nil {
			return 0, 0, err
		}
	} else {
		var rangeStart string
		var rangeEnd string

		queryTemplate := config.Query
		if queryTemplate == "" {
			queryTemplate = fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
				selectedColumns, parsedSrcTable.MySQL(), common.QuoteMySQLIdentifier(config.WatermarkColumn))
		}
		// Depending on the type of the range, convert the range into the correct type
		switch x := partition.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			rangeStart = strconv.FormatInt(x.IntRange.Start, 10)
			rangeEnd = strconv.FormatInt(x.IntRange.End, 10)
		case *protos.PartitionRange_UintRange:
			rangeStart = strconv.FormatUint(x.UintRange.Start, 10)
			rangeEnd = strconv.FormatUint(x.UintRange.End, 10)
		case *protos.PartitionRange_TimestampRange:
			rangeStart = "'" + x.TimestampRange.Start.AsTime().Format("2006-01-02 15:04:05.999999") + "'"
			rangeEnd = "'" + x.TimestampRange.End.AsTime().Format("2006-01-02 15:04:05.999999") + "'"
			if config.Query == "" {
				queryTemplate, rangeEnd = timestampRangeQuery(selectedColumns, parsedSrcTable.MySQL(),
					common.QuoteMySQLIdentifier(config.WatermarkColumn), x.TimestampRange.End.AsTime())
			}
		case *protos.PartitionRange_StringRange:
			rangeStart = "'" + escapeWithNoBackslashEscapes(x.StringRange.Start) + "'"
			rangeEnd = "'" + escapeWithNoBackslashEscapes(x.StringRange.End) + "'"
			if config.Query != "" {
				// custom query is only possible for standalone QRepFlowWorkflow;
				// this is a legacy feature and string partitioning is not supported
				return 0, 0, errors.New("can't construct a string range partition for custom queries")
			}
			if !x.StringRange.EndInclusive {
				queryTemplate = fmt.Sprintf(
					"SELECT %[1]s FROM %[2]s WHERE %[3]s >= {{.start}} AND %[3]s < {{.end}}",
					selectedColumns, parsedSrcTable.MySQL(), common.QuoteMySQLIdentifier(config.WatermarkColumn))
			}
		case *protos.PartitionRange_NullRange:
			if config.Query != "" {
				return 0, 0, errors.New("can't construct a null range partition for custom queries")
			}
			queryTemplate = fmt.Sprintf(
				"SELECT %s FROM %s WHERE %s IS NULL",
				selectedColumns, parsedSrcTable.MySQL(), common.QuoteMySQLIdentifier(config.WatermarkColumn),
			)
		default:
			return 0, 0, fmt.Errorf("unknown range type: %v", x)
		}

		templateParams := map[string]string{}
		if rangeStart != "" && rangeEnd != "" {
			templateParams["start"] = rangeStart
			templateParams["end"] = rangeEnd
		}

		// Build the query to pull records within the range from the source table
		// Be sure to order the results by the watermark column to ensure consistency across pulls
		query, err := utils.ExecuteTemplate(queryTemplate, templateParams)
		if err != nil {
			return 0, 0, err
		}

		if err := c.ExecuteSelectStreaming(ctx, query, &rs, onRow, onResult); err != nil {
			return 0, 0, err
		}
	}

	c.logger.Info("[mysql] pulled records",
		slog.Int64("records", totalRecords),
		slog.Int64("bytes", c.totalBytesRead.Load()),
		slog.Int("channelLen", len(stream.Records)))
	return totalRecords, c.deltaBytesRead.Swap(0), nil
}
