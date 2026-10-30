package connmysql

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var (
	mysqlCurrentTimestampRegex = regexp.MustCompile(`(?i)^(current_timestamp|now|localtimestamp|localtime)(\(\d?\))?$`)
	mysqlNumberRegex           = regexp.MustCompile(`^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$`)
	mysqlBitLiteralRegex       = regexp.MustCompile(`^b'([01]+)'$`)
)

// defaultExprFromInfoSchema renders information_schema.columns' COLUMN_DEFAULT (with EXTRA) as the literal that
// defaultExprFromMysqlColumnOption renders for DDL: numbers bare, strings single-quoted. A current-time default
// of a timestamp column gives CURRENT_TIMESTAMP, which destinations translate. Other expressions, NULL, and
// enums and sets (as on the DDL path) give false. MariaDB quotes literal defaults and spells NULL out; MySQL
// gives literals raw and marks expressions DEFAULT_GENERATED.
func defaultExprFromInfoSchema(qkind types.QValueKind, columnDefault string, extra string, mariaDB bool) (string, bool) {
	if qkind == types.QValueKindUint16Enum || qkind == types.QValueKindUint64Set {
		return "", false
	}
	if mysqlCurrentTimestampRegex.MatchString(columnDefault) {
		if qkind == types.QValueKindTimestamp || qkind == types.QValueKindTimestampTZ {
			return "CURRENT_TIMESTAMP", true
		}
		return "", false
	}

	value := columnDefault
	if mariaDB {
		if inner, ok := strings.CutPrefix(value, "'"); ok {
			if value, ok = strings.CutSuffix(inner, "'"); !ok || strings.Contains(value, `\`) {
				return "", false
			}
			value = strings.ReplaceAll(value, "''", "'")
		} else if !mysqlNumberRegex.MatchString(value) && !mysqlBitLiteralRegex.MatchString(value) {
			return "", false // NULL or an expression
		}
	} else if strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		return "", false
	}

	switch {
	case isNumericKind(qkind):
		if bits := mysqlBitLiteralRegex.FindStringSubmatch(value); bits != nil {
			n, err := strconv.ParseUint(bits[1], 2, 64)
			if err != nil {
				return "", false
			}
			return strconv.FormatUint(n, 10), true
		}
		if !mysqlNumberRegex.MatchString(value) {
			return "", false
		}
		return value, true
	case qkind == types.QValueKindString, qkind == types.QValueKindEnum, qkind == types.QValueKindDate,
		qkind == types.QValueKindTimestamp, qkind == types.QValueKindTimestampTZ, qkind == types.QValueKindTime:
		if strings.ContainsAny(value, "\\\t\n\r\x00") {
			return "", false
		}
		return "'" + strings.ReplaceAll(value, "'", "''") + "'", true
	default:
		return "", false
	}
}

// isNumericKind is whether a column of kind qkind takes a numeric default (TINYINT(1) read as a boolean included).
func isNumericKind(qkind types.QValueKind) bool {
	switch qkind {
	case types.QValueKindBoolean, types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32,
		types.QValueKindInt64, types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32,
		types.QValueKindUInt64, types.QValueKindFloat32, types.QValueKindFloat64, types.QValueKindNumeric:
		return true
	}
	return false
}
