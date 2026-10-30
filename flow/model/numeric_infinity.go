package model

import (
	"slices"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// WithoutNumericInfinity replaces numeric +-Infinity, in scalars and array elements, with NULL, as it was written
// before special values were kept: numeric only accepts infinity from Postgres 14 on, and an older destination would
// reject the whole batch. NaN, which numeric always accepts, and float infinities are kept.
func WithoutNumericInfinity(qv types.QValue) types.QValue {
	isInf := func(sv types.SpecialValue) bool { return sv == types.SpecialInfinity || sv == types.SpecialNegInfinity }
	switch v := qv.(type) {
	case types.QValueNumeric:
		if isInf(v.Special) {
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
