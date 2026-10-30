package types

import (
	"fmt"
	"reflect"
)

// SpecialValue is a value Postgres stores but Go's decimal and time types can't hold: NaN and +-Infinity numerics,
// +-infinity dates and timestamps. Sources set it (with a zero Val) only for Postgres destinations, which write it;
// other destinations keep getting NULL or a zero value from the source, as before.
type SpecialValue uint8

const (
	SpecialNone SpecialValue = iota
	SpecialNaN
	SpecialInfinity
	SpecialNegInfinity
)

// Text is the value's Postgres input form, which numeric, float, date and timestamp casts all accept.
func (s SpecialValue) Text() string {
	switch s {
	case SpecialNaN:
		return "NaN"
	case SpecialInfinity:
		return "Infinity"
	case SpecialNegInfinity:
		return "-Infinity"
	}
	return ""
}

// CheckArraySpecials returns an error if qv's special-element list doesn't have one entry per element, so the COPY
// (constructSpecialArray) fails the batch instead of panicking.
func CheckArraySpecials(qv QValue) error {
	specials := ArraySpecials(qv)
	if specials == nil {
		return nil
	}
	if rv := reflect.ValueOf(qv.Value()); rv.Kind() != reflect.Slice || rv.Len() != len(specials) {
		return fmt.Errorf("%s value has a special-element list of %d entries for %v", qv.Kind(), len(specials), qv.Value())
	}
	return nil
}

// ArraySpecials returns the special-element mask of an array value, or nil (none, or not an array that has one).
func ArraySpecials(qv QValue) []SpecialValue {
	switch v := qv.(type) {
	case QValueArrayNumeric:
		return v.Specials
	case QValueArrayDate:
		return v.Specials
	case QValueArrayTimestamp:
		return v.Specials
	case QValueArrayTimestampTZ:
		return v.Specials
	}
	return nil
}

// CompactSpecials returns specials, or nil when it marks nothing.
func CompactSpecials(specials []SpecialValue) []SpecialValue {
	for _, s := range specials {
		if s != SpecialNone {
			return specials
		}
	}
	return nil
}
