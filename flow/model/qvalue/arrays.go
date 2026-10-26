package qvalue

import (
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// NilElements returns which decoded array elements are nil (NULL), or nil if none are.
func NilElements(elems []any) []bool {
	var nulls []bool
	for i, el := range elems {
		if el == nil {
			if nulls == nil {
				nulls = make([]bool, len(elems))
			}
			nulls[i] = true
		}
	}
	return nulls
}

// TimeArrayOptions says what a date/timestamp/timestamptz array's +-infinity and NULL elements become.
type TimeArrayOptions struct {
	// Specials marks +-infinity elements (types.SpecialValue), for destinations that store infinity (Postgres)
	Specials bool
	// DefaultTime: on destinations without specials, +-infinity and NULL elements become DefaultTime(Destination),
	// as non-nullable scalars do, instead of the zero time (InternalVersion_ArrayConversionIntegrity)
	DefaultTime bool
	Destination protos.DBType
}

// PrepareTimeArray replaces the elements of a decoded date/timestamp/timestamptz array that aren't time.Time, so
// the element cast accepts them: pgx decodes +-infinity as pgtype.InfinityModifier and NULL as nil. Infinity
// becomes the zero time, marked in the returned specials with opts.Specials, or DefaultTime with opts.DefaultTime;
// NULL stays nil (the zero time, which destinations storing NULL elements mark from the NULL mask) unless
// opts.DefaultTime applies. elems is copied only if an element changes.
func PrepareTimeArray(elems []any, opts TimeArrayOptions) ([]any, []types.SpecialValue) {
	defaultTime := !opts.Specials && opts.DefaultTime
	var out []any
	var specials []types.SpecialValue
	set := func(i int, v any) {
		if out == nil {
			out = slices.Clone(elems)
		}
		out[i] = v
	}
	for i, el := range elems {
		switch v := el.(type) {
		case pgtype.InfinityModifier:
			switch {
			case opts.Specials:
				set(i, time.Time{})
				if specials == nil {
					specials = make([]types.SpecialValue, len(elems))
				}
				specials[i] = types.SpecialInfinity
				if v == pgtype.NegativeInfinity {
					specials[i] = types.SpecialNegInfinity
				}
			case defaultTime:
				set(i, DefaultTime(opts.Destination))
			default:
				set(i, time.Time{})
			}
		case nil:
			if defaultTime {
				set(i, DefaultTime(opts.Destination))
			}
		}
	}
	if out == nil {
		out = elems
	}
	return out, specials
}

// NumericSpecial marks NaN and +-Infinity numerics, which decimal.Decimal can't hold. Infinities only count without a
// precision: Postgres can't store them in numeric(p, s).
func NumericSpecial(numVal pgtype.Numeric, precision int16) types.SpecialValue {
	switch {
	case !numVal.Valid:
		return types.SpecialNone
	case numVal.NaN:
		return types.SpecialNaN
	case precision != 0:
		return types.SpecialNone
	case numVal.InfinityModifier == pgtype.Infinity:
		return types.SpecialInfinity
	case numVal.InfinityModifier == pgtype.NegativeInfinity:
		return types.SpecialNegInfinity
	}
	return types.SpecialNone
}
