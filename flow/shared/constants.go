package shared

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

var Year0000 = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)

// PeerDB Internal Version System
//
// This versioning system allows PeerDB to introduce breaking changes to data formats,
// connector behavior, and destination-specific settings in a backward-compatible way.
//
// ## How it works:
//   - Each mirror is created with an internal version number stored in the
//     FlowConnectionConfigsCore.Version field in the catalog database
//   - The version is set once when a mirror is created and persists for the lifetime of that mirror
//   - New mirrors are created with InternalVersion_Latest
//   - Existing mirrors continue using their original version, ensuring stable behavior across upgrades
//
// ## How to add a new version:
//  1. Add a new constant below with a descriptive name and a comment explaining the change
//  2. Add version checks in the relevant connector code (e.g., if version >= InternalVersion_MyFeature)
//  3. Add e2e test to verify both old and new versions work correctly (old mirrors keep old behavior, new mirrors get new behavior)

const (
	InternalVersion_First uint32 = iota
	// Postgres: vector types ("vector", "halfvec", "sparsevec") replicated as float arrays instead of string
	InternalVersion_PgVectorAsFloatArray
	// MongoDB: rename `_full_document` column to `doc`
	InternalVersion_MongoDBFullDocumentColumnToDoc
	// All: setting json_type_escape_dots_in_keys = true when inserting JSON column to ClickHouse (only impacts MongoDB today)
	InternalVersion_JsonEscapeDotsInKeys
	// MongoDB: `_id` column values stored as-is without redundant quotes
	InternalVersion_MongoDBIdWithoutRedundantQuotes
	// MySQL: convert enums to integers for older versions without binlog row metadata support
	InternalVersion_MySQL5ConvertEnumsToInts
	// MySQL: convert BIT to UInt64
	InternalVersion_MySQLConvertBitToUInt64
	// MySQL: convert sets to integers for older versions without binlog row metadata support
	InternalVersion_MySQL5ConvertSetsToInts
	// All: infer date-like and timestamp-like string fields in JSON columns as DateTime64 when
	// inserting into ClickHouse. Otherwise inference is indeterministic depending on initial
	// input data, and can silently corrupts out-of-range values (e,g, pre-1970 date becomes 1970-01-01).
	InternalVersion_AlwaysUseDateTime64Inference
	// MySQL: empty (non-NULL) binary values (BLOB, VARBINARY, CHARACTER SET binary strings) replicated
	// as empty bytes instead of NULL. Older mirrors emit a nil slice for them, which sinks write as NULL.
	InternalVersion_MySQLEmptyBinaryAsEmpty
	// MySQL: an update that changes the primary key is replicated as a delete of the old row plus an
	// insert of the new one. Older mirrors emit an update, which normalize applies by the new key only,
	// leaving the old row behind.
	InternalVersion_MySQLPKeyChangeAsDeleteInsert
	// Postgres: unsigned integers created wide enough for their range (UInt16 -> INTEGER, UInt32 -> BIGINT).
	// UInt64 stays BIGINT: NUMERIC(20,0) is lossless but can't be an identity column, so it's the user's
	// choice (pre-create or alter the target column), which InternalVersion_PgNormalizeCastToTargetTypes honours.
	InternalVersion_PgWidenUnsignedInts
	// Postgres: normalize casts values to the target columns' own types instead of PeerDB's mapped types, so
	// columns widened or pre-created by the user hold through CDC, as they already do for the snapshot.
	InternalVersion_PgNormalizeCastToTargetTypes
	// MySQL, every destination: dates with any zero part (a zero month or day, i.e. partial zero dates, or a
	// zero year) replicated like zero dates (the epoch). Older mirrors roll a zero month or day forward to 01,
	// inventing a real date.
	InternalVersion_MySQLZeroDatePartsAsZero
	// Postgres: a NULL array stays NULL through CDC normalize. Older mirrors write '{}' (ARRAY(SELECT
	// JSON_ARRAY_ELEMENTS_TEXT(NULL)) is empty), while their snapshot kept NULL.
	InternalVersion_PgNullArrayStaysNull
	// CockroachDB: NULL elements inside arrays are marked (types.ArrayNulls), and Postgres destinations write them as
	// NULL. Older mirrors write the element type's zero value ({1,NULL,3} -> {1,0,3}, text '', bool false, ...).
	InternalVersion_ArrayNullElements
	// CockroachDB: a batch stays open past its sync interval and batch size until a resolved timestamp advances the
	// cursor (for at most an hour). Older mirrors end the batch at the deadline with the cursor unchanged, so a
	// transaction or a catch-up that takes longer to emit than the batch window replays forever (#22, #31).
	InternalVersion_CockroachHoldBatchForResolved

	TotalNumberOfInternalVersions
	InternalVersion_Latest = TotalNumberOfInternalVersions - 1
)

type (
	ContextKey  string
	TaskQueueID string
)

func (c ContextKey) String() string {
	return string(c)
}

const (
	// Task Queues
	PeerFlowTaskQueue        TaskQueueID = "peer-flow-task-queue"
	SnapshotFlowTaskQueue    TaskQueueID = "snapshot-flow-task-queue"
	MaintenanceFlowTaskQueue TaskQueueID = "maintenance-flow-task-queue"

	// Queries
	CDCFlowStateQuery  = "q-cdc-flow-state"
	QRepFlowStateQuery = "q-qrep-flow-state"
)

var MirrorNameSearchAttribute = temporal.NewSearchAttributeKeyString("MirrorName")

func NewSearchAttributes(mirrorName string) temporal.SearchAttributes {
	return temporal.NewSearchAttributes(MirrorNameSearchAttribute.ValueSet(mirrorName))
}

const (
	FlowNameKey      ContextKey = "flowName"
	PartitionIDKey   ContextKey = "partitionId"
	DeploymentUIDKey ContextKey = "deploymentUid"
	RequestIdKey     ContextKey = "x-peerdb-request-id"
)

const (
	QRepFetchSize   = 128 * 1024
	QRepChannelSize = 1024
)

const CDCResyncTableSuffix = "_resync"

// SnowflakeClearValueThresholdBytes leaves headroom below Snowflake's 16 MiB value limit.
const SnowflakeClearValueThresholdBytes = 15 * 1024 * 1024

func Val[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// Flag constants for flow config Flags mapping
const (
	Flag_ClickHouseTime64Enabled = "clickhouse_time64_enabled"
)
