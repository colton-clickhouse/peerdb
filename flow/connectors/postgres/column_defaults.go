package connpostgres

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var (
	pgZeroDateDefaultRegex = regexp.MustCompile(`^'(0000-|\d{4}-00-|\d{4}-\d{2}-00)`)
	pgTimeOfDayRegex       = regexp.MustCompile(`^'([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?'$`)
)

// pgDefaultExpr translates a source column default (DefaultExpr: numbers bare, strings single-quoted, or
// CURRENT_TIMESTAMP) into a Postgres DEFAULT expression for a column of kind qkind created as pgType, or false
// when Postgres would reject it or it would mean something else there.
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
	switch qkind {
	case types.QValueKindBoolean:
		if defaultExpr == "true" || defaultExpr == "false" {
			return defaultExpr, true
		}
		if n, err := strconv.ParseFloat(defaultExpr, 64); err == nil {
			return strconv.FormatBool(n != 0), true // MySQL TINYINT(1)
		}
		return "", false
	case types.QValueKindDate, types.QValueKindTimestamp, types.QValueKindTimestampTZ:
		if pgZeroDateDefaultRegex.MatchString(defaultExpr) {
			return "", false // MySQL zero dates
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
