package model

import (
	"slices"

	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// WithoutNumericInfinity replaces numeric +-Infinity, which numeric only accepts from Postgres 14 on (an older
// destination would reject the whole batch): a scalar with NULL, or with 0 when its column isn't nullable (a NULL
// would fail a NOT NULL or key column just the same), and an array element with a NULL element. NaN, which numeric
// always accepts, and float infinities (float columns take them on every version) are kept.
func WithoutNumericInfinity(qv types.QValue, nullable bool) types.QValue {
	isInf := func(sv types.SpecialValue) bool { return sv == types.SpecialInfinity || sv == types.SpecialNegInfinity }
	switch v := qv.(type) {
	case types.QValueNumeric:
		if isInf(v.Special) {
			if !nullable {
				v.Val, v.Special = decimal.Zero, types.SpecialNone
				return v
			}
			return types.QValueNull(types.QValueKindNumeric)
		}
	case types.QValueArrayNumeric:
		if !slices.ContainsFunc(v.Specials, isInf) {
			return qv
		}
		specials := slices.Clone(v.Specials)
		nulls := make([]bool, len(v.Val))
		copy(nulls, v.Nulls)
		for i, sv := range specials {
			if isInf(sv) {
				specials[i], nulls[i] = types.SpecialNone, true
			}
		}
		v.Specials, v.Nulls = specials, nulls
		return v
	}
	return qv
}
