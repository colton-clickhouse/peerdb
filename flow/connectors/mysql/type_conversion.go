package connmysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func QkindFromMysqlColumnType(ct string, binlogRowMetadataSupported bool, version uint32) (types.QValueKind, error) {
	if mysql_validation.IsCompressedColumnType(ct) {
		return types.QValueKindInvalid, fmt.Errorf("MariaDB COMPRESSED columns are not supported: %s", ct)
	}
	// https://mariadb.com/docs/server/reference/data-types/date-and-time-data-types/timestamp#tab-current-1
	ct, _ = strings.CutSuffix(ct, " /* mariadb-5.3 */")
	ct, _ = strings.CutSuffix(ct, " zerofill")
	ct, isUnsigned := strings.CutSuffix(ct, " unsigned")
	ct, param, _ := strings.Cut(ct, "(")
	switch strings.ToLower(ct) {
	case "json":
		return types.QValueKindJSON, nil
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"clob", "varchar2", // maria
		"xmltype": // maria
		return types.QValueKindString, nil
	case "set":
		// keeps snapshot and cdc consistent for mysql without row metadata
		if !binlogRowMetadataSupported && version >= shared.InternalVersion_MySQL5ConvertSetsToInts {
			return types.QValueKindUint64Set, nil
		}
		return types.QValueKindString, nil
	case "enum":
		if !binlogRowMetadataSupported && version >= shared.InternalVersion_MySQL5ConvertEnumsToInts {
			return types.QValueKindUint16Enum, nil
		}
		return types.QValueKindEnum, nil
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob",
		"char byte", "raw": // maria
		return types.QValueKindBytes, nil
	case "date":
		return types.QValueKindDate, nil
	case "datetime", "timestamp":
		return types.QValueKindTimestamp, nil
	case "time":
		return types.QValueKindTime, nil
	case "decimal", "numeric",
		"number": // maria (Oracle-mode synonym for decimal; TiDB parser rejects the spelling)
		return types.QValueKindNumeric, nil
	case "float":
		return types.QValueKindFloat32, nil
	case "double":
		return types.QValueKindFloat64, nil
	case "tinyint":
		if strings.HasPrefix(param, "1)") {
			return types.QValueKindBoolean, nil
		} else if isUnsigned {
			return types.QValueKindUInt8, nil
		} else {
			return types.QValueKindInt8, nil
		}
	case "smallint", "year":
		if isUnsigned {
			return types.QValueKindUInt16, nil
		} else {
			return types.QValueKindInt16, nil
		}
	case "mediumint", "int":
		if isUnsigned {
			return types.QValueKindUInt32, nil
		} else {
			return types.QValueKindInt32, nil
		}
	case "bit":
		return types.QValueKindUInt64, nil
	case "bigint":
		if isUnsigned {
			return types.QValueKindUInt64, nil
		} else {
			return types.QValueKindInt64, nil
		}
	case "vector":
		return types.QValueKindArrayFloat32, nil
	case "uuid": // maria
		return types.QValueKindUUID, nil
	case "inet4", "inet6": // maria
		return types.QValueKindINET, nil
	case "geometry", "point", "polygon", "linestring", "multipoint", "multilinestring", "multipolygon", "geomcollection", "geometrycollection":
		return types.QValueKindGeometry, nil
	default:
		return types.QValueKind(""), fmt.Errorf("unknown mysql type %s", ct)
	}
}

// shouldReportColumnTypeChange decides whether a difference between the schema's qkind and
// the qkind derived from the TABLE_MAP_EVENT wire type is a genuine column type change worth
// reporting, given the server flavor.
//
// The one expected, benign difference is MariaDB's uuid/inet4/inet6 types: verified against
// MariaDB 11.8, they all arrive with ColumnType 0xFE (MYSQL_TYPE_STRING) and binary charset —
// byte-for-byte indistinguishable from BINARY(N) — so qkindFromMysqlType maps them to bytes.
// That bytes-from-the-wire kind legitimately differs from the schema's uuid/inet kind, so the
// pairing is suppressed. The suppression is scoped narrowly: only MariaDB, only when the wire
// kind is exactly bytes, and only when the schema kind is uuid/inet — so a real change such as
// schema uuid -> wire integer is still reported.
// tinyInt1Kind is the kind a TINYINT(1) column (read as a boolean by QkindFromMysqlColumnType) is decoded as:
// an integer when its column setting's destination_type is an integer type, or, without one naming an integer
// or boolean type, when the mirror reads every TINYINT(1) as an integer (PEERDB_MYSQL_TINYINT1_AS_INT).
// Other kinds are returned unchanged.
func tinyInt1Kind(qkind types.QValueKind, columnType string, setting *protos.ColumnSetting, mirrorAsInt bool) types.QValueKind {
	if qkind != types.QValueKindBoolean || !strings.HasPrefix(strings.ToLower(columnType), "tinyint(1)") {
		return qkind
	}
	asInt := mirrorAsInt
	if setting != nil {
		switch destinationType := strings.ToLower(strings.TrimSpace(setting.DestinationType)); {
		case isIntegerTypeName(destinationType):
			asInt = true
		case destinationType == "boolean" || destinationType == "bool":
			asInt = false
		}
	}
	if !asInt {
		return qkind
	}
	if strings.Contains(strings.ToLower(columnType), "unsigned") {
		return types.QValueKindUInt8
	}
	return types.QValueKindInt8
}

// tinyInt1Pinned reads a mirror's PEERDB_MYSQL_TINYINT1_AS_INT from its own env only: mirror creation pins the
// setting there, so a change of the global setting can't make later tables, partitions or ADD COLUMNs decode
// TINYINT(1) another way. Without it (a mirror from before the setting existed) TINYINT(1) stays boolean, column
// settings included, as such mirrors always decoded it.
func tinyInt1Pinned(env map[string]string) (pinned bool, asInt bool, err error) {
	v, ok := env["PEERDB_MYSQL_TINYINT1_AS_INT"]
	if !ok {
		return false, false, nil
	}
	asInt, err = strconv.ParseBool(v)
	if err != nil {
		return false, false, fmt.Errorf("invalid PEERDB_MYSQL_TINYINT1_AS_INT %q: %w", v, err)
	}
	return true, asInt, nil
}

// isIntegerTypeName reports whether a lower-cased destination_type names an integer type on Postgres or ClickHouse
func isIntegerTypeName(name string) bool {
	switch name {
	case "smallint", "int2", "integer", "int", "int4", "bigint", "int8",
		"int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64":
		return true
	}
	return false
}

func shouldReportColumnTypeChange(schemaKind, wireKind types.QValueKind, flavor protos.MySqlFlavor) bool {
	if schemaKind == wireKind {
		return false
	}
	if flavor == protos.MySqlFlavor_MYSQL_MARIA && wireKind == types.QValueKindBytes &&
		(schemaKind == types.QValueKindUUID || schemaKind == types.QValueKindINET) {
		return false
	}
	return true
}
