package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	pkg_pg "github.com/PeerDB-io/peerdb/flow/pkg/postgres"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *PostgresConnector) ValidateCheck(ctx context.Context) error {
	if err := pkg_pg.CheckUnsupportedDatabase(ctx, c.conn); err != nil {
		return err
	}

	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return err
	}

	if pgversion < shared.POSTGRES_12 {
		return fmt.Errorf("postgres must be of PG12 or above. Current version: %d", pgversion)
	}
	return nil
}

func (c *PostgresConnector) CheckSourceTables(
	ctx context.Context,
	tableNames []*common.QualifiedTable,
	tableMappings []*protos.TableMapping,
	pubName string,
	noCDC bool,
) error {
	if c.conn == nil {
		return fmt.Errorf("check tables: conn is nil")
	}

	// Check that we can select from all tables
	tableArr := make([]string, 0, len(tableNames))
	var missingTables []common.QualifiedTable
	for idx, parsedTable := range tableNames {
		var row pgx.Row
		tableArr = append(tableArr, fmt.Sprintf(`(%s::text,%s::text)`,
			utils.QuoteLiteral(parsedTable.Namespace), utils.QuoteLiteral(parsedTable.Table)))

		selectedColumnsStr := "*"
		if excludedColumns := tableMappings[idx].Exclude; len(excludedColumns) != 0 {
			selectedColumns, err := c.GetSelectedColumns(ctx, parsedTable, excludedColumns)
			if err != nil {
				return fmt.Errorf("failed to get selected columns for SELECT check: %w", err)
			}

			for i, col := range selectedColumns {
				selectedColumns[i] = common.QuoteIdentifier(col)
			}

			selectedColumnsStr = strings.Join(selectedColumns, ", ")
		}
		if err := c.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT %s FROM %s LIMIT 0", selectedColumnsStr, parsedTable),
		).Scan(&row); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == pgerrcode.UndefinedTable {
				missingTables = append(missingTables, *parsedTable)
				continue
			}
			return fmt.Errorf("failed to select from table %s: %w", parsedTable, err)
		}
	}
	if len(missingTables) > 0 {
		return common.NewSourceTablesMissingError(missingTables)
	}

	if pubName != "" && !noCDC {
		// Check if publication exists
		var alltables bool
		if err := c.conn.QueryRow(ctx, "SELECT puballtables FROM pg_publication WHERE pubname=$1", pubName).Scan(&alltables); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("publication does not exist: %s", pubName)
			}
			return fmt.Errorf("error while checking for publication existence: %w", err)
		}

		if !alltables {
			// Check if tables belong to publication
			tableStr := strings.Join(tableArr, ",")

			rows, err := c.conn.Query(
				ctx,
				fmt.Sprintf(`select schemaname,tablename
				from (values %s) as input(schemaname,tablename)
				where not exists (
					select * from pg_publication_tables pub
					where pubname=$1 and pub.schemaname=input.schemaname and pub.tablename=input.tablename
				)`, tableStr),
				pubName,
			)
			if err != nil {
				return err
			}
			missing, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (common.QualifiedTable, error) {
				var qt common.QualifiedTable
				if err := row.Scan(&qt.Namespace, &qt.Table); err != nil {
					return common.QualifiedTable{}, err
				}
				return qt, nil
			})
			if err != nil {
				return err
			}

			if len(missing) != 0 {
				return common.NewTablesNotInPublicationError(pubName, missing)
			}
		}
	}

	return nil
}

func (c *PostgresConnector) CheckReplicationPermissions(ctx context.Context, username string) error {
	if c.conn == nil {
		return fmt.Errorf("check replication permissions: conn is nil")
	}

	// check wal_level
	var walLevel string
	if err := c.conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return err
	}

	if walLevel != "logical" {
		return fmt.Errorf("wal_level is not logical")
	}

	var replicationRes bool
	err := c.conn.QueryRow(ctx, "SELECT rolreplication FROM pg_roles WHERE rolname = $1", username).Scan(&replicationRes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.logger.Warn("No rows in pg_roles for user. Skipping rolereplication check",
				"username", username)
		} else {
			return err
		}
	}

	if !replicationRes {
		// RDS case: check pg_settings for rds.logical_replication
		var setting string
		err := c.conn.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name = 'rds.logical_replication'").Scan(&setting)
		if !errors.Is(err, pgx.ErrNoRows) {
			if err != nil || setting != "on" {
				return fmt.Errorf("rds.logical_replication setting must be enabled")
			}
		}
	}

	// max_wal_senders must be at least 2
	var insufficientMaxWalSenders bool
	if err := c.conn.QueryRow(ctx,
		"SELECT setting::int<2 FROM pg_settings WHERE name='max_wal_senders'",
	).Scan(&insufficientMaxWalSenders); err != nil {
		return err
	}

	if insufficientMaxWalSenders {
		return fmt.Errorf("max_wal_senders must be at least 2")
	}

	serverVersion, err := shared.GetMajorVersion(ctx, c.conn)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}

	if recoveryRes, err := c.IsInRecovery(ctx); err != nil {
		return fmt.Errorf("failed to check if Postgres is in recovery: %w", err)
	} else if recoveryRes {
		if serverVersion < shared.POSTGRES_16 {
			return fmt.Errorf("cannot create replication slots on a standby server with version <16")
		}

		var hsFeedback bool
		if err := c.conn.QueryRow(
			ctx, "SELECT setting::bool FROM pg_settings WHERE name='hot_standby_feedback'",
		).Scan(&hsFeedback); err != nil {
			return fmt.Errorf("failed to check hot_standby_feedback: %w", err)
		}
		if !hsFeedback {
			return fmt.Errorf("hot_standby_feedback setting must be enabled on standby servers")
		}
	}

	return nil
}

func (c *PostgresConnector) CheckReplicationConnectivity(ctx context.Context, env map[string]string) error {
	// Check if we can create a replication connection
	conn, _, err := c.CreateReplConn(ctx, env)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "IDENTIFY_SYSTEM"); err != nil {
		return fmt.Errorf("failed to execute IDENTIFY_SYSTEM on replication connection: %w", err)
	}

	return conn.Close(ctx)
}

func (c *PostgresConnector) CheckPublicationCreationPermissions(ctx context.Context, srcTableNames []string) error {
	pubName := "_peerdb_tmp_test_publication_" + common.RandomString(5)
	if err := c.CreatePublication(ctx, srcTableNames, pubName); err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx, "DROP PUBLICATION "+pubName); err != nil {
		return fmt.Errorf("failed to drop publication: %v", err)
	}
	return nil
}

func (c *PostgresConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	noCDC := cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly
	if !noCDC {
		// Check replication connectivity
		if err := c.CheckReplicationConnectivity(ctx, cfg.Env); err != nil {
			return fmt.Errorf("unable to establish replication connectivity: %w", err)
		}

		// Check permissions of postgres peer
		if err := c.CheckReplicationPermissions(ctx, c.Config.User); err != nil {
			return fmt.Errorf("failed to check replication permissions: %w", err)
		}
	}

	sourceTables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		parsedTable, parseErr := common.ParseTableIdentifier(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			return fmt.Errorf("invalid source table identifier: %w", parseErr)
		}

		sourceTables = append(sourceTables, parsedTable)
	}

	pubName := cfg.PublicationName
	// Check source tables before the publication, for better errors
	if err := c.CheckSourceTables(ctx, sourceTables, cfg.TableMappings, pubName, noCDC); err != nil {
		return fmt.Errorf("provided source tables invalidated: %w", err)
	}

	if pubName == "" && !noCDC {
		srcTableNames := make([]string, 0, len(sourceTables))
		for _, srcTable := range sourceTables {
			srcTableNames = append(srcTableNames, srcTable.String())
		}

		if err := c.CheckPublicationCreationPermissions(ctx, srcTableNames); err != nil {
			return fmt.Errorf("invalid publication creation permissions: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) ValidateMirrorDestination(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) error {
	// before the resync shortcut: recreating the tables doesn't make a keyless one replicable
	if !cfg.InitialSnapshotOnly {
		sourceTables := make([]string, 0, len(cfg.TableMappings))
		for _, tableMapping := range cfg.TableMappings {
			sourceTables = append(sourceTables, tableMapping.SourceTableIdentifier)
		}
		if err := keylessTablesError(sourceTables, tableNameSchemaMapping); err != nil {
			return fmt.Errorf("%w, or use an initial snapshot only mirror", err)
		}
	}

	// also before the resync shortcut: recreated tables would get the same truncated names
	if err := longIdentifiersError(cfg.TableMappings, tableNameSchemaMapping); err != nil {
		return err
	}
	if err := columnTypeSettingsError(cfg.TableMappings, tableNameSchemaMapping, cfg.Version); err != nil {
		return err
	}

	if cfg.Resync {
		return nil // no need to validate schema for resync, as we will create or replace the tables
	}

	if cfg.System == protos.TypeSystem_PG && cfg.Env["PEERDB_PG_AUTOMATED_SCHEMA_DUMP"] == "true" {
		return nil // pg_dump will create the schema and tables on the destination
	}

	customTypeMapping, err := c.fetchCustomTypeMapping(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch custom type mapping: %w", err)
	}

	// Validate that all source columns exist in destination tables
	checkedSchemas := make(map[string]struct{})
	for _, tableMapping := range cfg.TableMappings {
		srcTableIdentifier := tableMapping.SourceTableIdentifier
		dstTableIdentifier := tableMapping.DestinationTableIdentifier

		if dstTableIdentifier == "" {
			return errors.New("destination table identifier is empty")
		}

		// Get the source table schema
		srcSchema, ok := tableNameSchemaMapping[srcTableIdentifier]
		if !ok {
			return fmt.Errorf("source table %s not found in schema mapping", srcTableIdentifier)
		}

		// Get destination table columns with types
		dstColumns, err := pkg_pg.GetDestinationTableSchema(ctx, c.conn, dstTableIdentifier, customTypeMapping)
		if err != nil {
			// If table doesn't exist, check that the schema does before continuing
			if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), "does not exist") {
				parsedDst, parseErr := common.ParseTableIdentifier(dstTableIdentifier)
				if parseErr != nil {
					return fmt.Errorf("invalid destination table identifier %s: %w", dstTableIdentifier, parseErr)
				}
				if _, alreadyChecked := checkedSchemas[parsedDst.Namespace]; !alreadyChecked {
					if schemaErr := pkg_pg.CheckSchemaExists(ctx, c.conn, parsedDst.Namespace); schemaErr != nil {
						return schemaErr
					}
					checkedSchemas[parsedDst.Namespace] = struct{}{}
				}
				continue
			}
			return fmt.Errorf("failed to get columns for destination table %s: %w", dstTableIdentifier, err)
		}

		if cfg.DoInitialSnapshot {
			// Check if destination table already has rows
			if err := pkg_pg.CheckTableEmpty(ctx, c.conn, dstTableIdentifier); err != nil {
				return err
			}
		}

		// Check if all source columns exist in destination with matching types
		for _, srcField := range srcSchema.Columns {
			colName := srcField.Name

			// Skip excluded columns
			if slices.Contains(tableMapping.Exclude, colName) {
				continue
			}

			// Resolve rename if present
			columnMappings := make([]pkg_pg.ColumnMapping, 0, len(tableMapping.Columns))
			for _, col := range tableMapping.Columns {
				columnMappings = append(columnMappings, pkg_pg.ColumnMapping{
					SourceName:      col.SourceName,
					DestinationName: col.DestinationName,
				})
			}
			colName = pkg_pg.ResolveDestinationColumnName(colName, columnMappings)

			// Check if the column exists in destination
			if err := pkg_pg.CheckColumnExists(srcField.Name, colName, dstColumns, dstTableIdentifier); err != nil {
				return err
			}

			// Check type compatibility when using the PG type system
			if cfg.System == protos.TypeSystem_PG {
				dstCol := dstColumns[colName]
				if err := pkg_pg.CheckColumnTypeCompatibility(
					srcField.Name, srcField.Type, srcField.TypeModifier,
					colName, dstCol.TypeName, dstCol.TypeMod,
					dstTableIdentifier,
				); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// keylessTablesError names the tables among names whose schema has no primary key columns, or is nil. Their
// changes can't be applied to Postgres: normalize matches rows by primary key, and as a batch normalizes all
// its tables together, a keyless table's malformed MERGE would fail every table's. A Postgres source reports
// all columns as the key of a REPLICA IDENTITY FULL table, so only tables with nothing to match on count.
func keylessTablesError(names []string, schemas map[string]*protos.TableSchema) error {
	var keyless []string
	for _, name := range names {
		if schema, ok := schemas[name]; ok && len(schema.PrimaryKeyColumns) == 0 {
			keyless = append(keyless, name)
		}
	}
	if len(keyless) == 0 {
		return nil
	}
	return fmt.Errorf("tables without a primary key can't be replicated with CDC to Postgres: %s; "+
		"add a primary key on the source or remove them from the mirror", strings.Join(keyless, ", "))
}

// maxIdentifierBytes is Postgres's identifier limit (NAMEDATALEN - 1)
const maxIdentifierBytes = 63

// longIdentifiersError names the destination schemas, tables and columns longer than maxIdentifierBytes, or
// is nil. Postgres truncates longer names silently, so the target's names would differ from the source's,
// and two names sharing their first 63 bytes collide, which fails table setup for the whole mirror.
func longIdentifiersError(tableMappings []*protos.TableMapping, schemas map[string]*protos.TableSchema) error {
	var long []string
	for _, tableMapping := range tableMappings {
		dst := tableMapping.DestinationTableIdentifier
		namespace, table, _ := strings.Cut(dst, ".")
		if len(namespace) > maxIdentifierBytes || len(table) > maxIdentifierBytes {
			long = append(long, "table "+dst)
		}
		if schema, ok := schemas[tableMapping.SourceTableIdentifier]; ok {
			for _, column := range schema.Columns {
				if len(column.Name) > maxIdentifierBytes && !slices.Contains(tableMapping.Exclude, column.Name) {
					long = append(long, "column "+dst+"."+column.Name)
				}
			}
		}
	}
	if len(long) == 0 {
		return nil
	}
	return fmt.Errorf("identifiers longer than %d bytes can't be replicated to Postgres, which would truncate them: %s; "+
		"rename them on the source, map the table to a shorter destination name, or exclude the columns",
		maxIdentifierBytes, strings.Join(long, ", "))
}

// columnTypeSettingsError names the column settings whose destination_type Postgres doesn't apply, or is nil.
// Postgres creates columns from the source's types; destination_type only picks a MySQL TINYINT(1) column's
// decoding (smallint or boolean), which the source schema then carries. A destination_type naming the type the
// column gets anyway is accepted. Tables without a schema (resync doesn't read it) aren't checked: their
// settings were checked when they were added.
func columnTypeSettingsError(
	tableMappings []*protos.TableMapping, schemas map[string]*protos.TableSchema, version uint32,
) error {
	var unsupported []string
	for _, tableMapping := range tableMappings {
		schema, ok := schemas[tableMapping.SourceTableIdentifier]
		if !ok {
			continue
		}
		kinds := make(map[string]types.QValueKind, len(schema.Columns))
		for _, column := range schema.Columns {
			kinds[column.Name] = types.QValueKind(column.Type)
		}
		for _, setting := range tableMapping.Columns {
			if setting.DestinationType == "" {
				continue
			}
			if kind, ok := kinds[setting.SourceName]; ok &&
				pgTypeAlias(setting.DestinationType) == pgTypeAlias(qValueKindToPostgresType(string(kind), version)) {
				continue
			}
			unsupported = append(unsupported,
				fmt.Sprintf("%s.%s %s", tableMapping.SourceTableIdentifier, setting.SourceName, setting.DestinationType))
		}
	}
	if len(unsupported) == 0 {
		return nil
	}
	return fmt.Errorf("column destination types aren't applied for Postgres destinations: %s; "+
		"the only one supported is smallint or boolean on a MySQL TINYINT(1) column; remove the others "+
		"(pre-create the target table for other types)", strings.Join(unsupported, ", "))
}

// pgTypeAlias spells a Postgres type name one way, for comparing names.
func pgTypeAlias(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	switch name {
	case "int2":
		return "smallint"
	case "int", "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "bool":
		return "boolean"
	case "float4":
		return "real"
	case "float8", "float":
		return "double precision"
	}
	return name
}
