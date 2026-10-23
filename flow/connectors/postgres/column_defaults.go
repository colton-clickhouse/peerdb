package connpostgres

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var (
	pgZeroDateDefaultRegex = regexp.MustCompile(`^'(0000-|\d{4}-00-|\d{4}-\d{2}-00)`)
	pgTimeOfDayRegex       = regexp.MustCompile(`^'([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?'$`)
)

// pgDefaultExpr translates a source column default (DefaultExpr: numbers bare, strings single-quoted, or
// CURRENT_TIMESTAMP) into a Postgres DEFAULT expression for a column of kind qkind created as pgType, or false
// when Postgres would reject it or it would mean something else there. A default left out costs the target a
// default; one Postgres accepts but can't use (out of range, an impossible date) would break the application's
// inserts after cutover, or table setup.
func pgDefaultExpr(defaultExpr string, qkind types.QValueKind, pgType string) (string, bool) {
	if defaultExpr == "CURRENT_TIMESTAMP" {
		switch qkind {
		case types.QValueKindTimestamp:
			// replicated timestamps are UTC, whatever the session's time zone
			return "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", true
		case types.QValueKindTimestampTZ:
			return "CURRENT_TIMESTAMP", true
		}
		return "", false
	}
	if strings.Contains(defaultExpr, `\`) {
		// the MySQL reader never renders a backslash; a Postgres source doubles them, which Postgres reads literally
		return "", false
	}
	literal, quoted := strings.CutPrefix(defaultExpr, "'")
	if quoted {
		literal = strings.TrimSuffix(literal, "'")
	}
	switch qkind {
	case types.QValueKindBoolean:
		if defaultExpr == "true" || defaultExpr == "false" {
			return defaultExpr, true
		}
		if n, err := strconv.ParseFloat(literal, 64); err == nil {
			return strconv.FormatBool(n != 0), true // MySQL TINYINT(1), also given as '0' or '1'
		}
		return "", false
	case types.QValueKindInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindUInt8, types.QValueKindUInt16, types.QValueKindUInt32, types.QValueKindUInt64:
		if bits, ok := pgIntegerBits[strings.ToUpper(pgType)]; ok {
			if _, err := strconv.ParseInt(literal, 10, bits); err != nil {
				return "", false // out of the target's range, e.g. BIGINT UNSIGNED's maximum in bigint
			}
		}
	case types.QValueKindDate, types.QValueKindTimestamp:
		if pgZeroDateDefaultRegex.MatchString(defaultExpr) {
			return "", false // MySQL zero dates
		}
		if quoted && !pgValidDateTime(literal) {
			return "", false // MySQL accepts impossible dates under ALLOW_INVALID_DATES
		}
	case types.QValueKindTime:
		if !strings.EqualFold(pgType, "INTERVAL") && !pgTimeOfDayRegex.MatchString(defaultExpr) {
			return "", false // a duration time can't hold
		}
	case types.QValueKindBytes, types.QValueKindGeometry, types.QValueKindGeography, types.QValueKindPoint:
		return "", false // string literals mean something else for these
	}
	return defaultExpr, true
}

var pgIntegerBits = map[string]int{"SMALLINT": 16, "INT2": 16, "INTEGER": 32, "INT4": 32, "BIGINT": 64, "INT8": 64}

// pgValidDateTime is whether a date or timestamp literal is a real date and time.
func pgValidDateTime(literal string) bool {
	for _, layout := range []string{time.DateOnly, "2006-01-02 15:04:05.999999999"} {
		if _, err := time.Parse(layout, literal); err == nil {
			return true
		}
	}
	return false
}
