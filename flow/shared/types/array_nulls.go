package types

import (
	"fmt"
	"reflect"
)

// Array values hold their elements in plain slices, so a NULL element is stored as the element type's zero value.
// Sources that can tell set the Nulls mask alongside; destinations that can store NULL elements (Postgres) read it,
// the others keep writing the zero value. A nil mask means no NULL elements.

// ArrayNulls returns the NULL-element mask of an array value, or nil (no NULL elements, or not an array).
func ArrayNulls(qv QValue) []bool {
	switch v := qv.(type) {
	case QValueArrayInterval:
		return v.Nulls
	case QValueArrayUUID:
		return v.Nulls
	case QValueArrayFloat32:
		return v.Nulls
	case QValueArrayFloat64:
		return v.Nulls
	case QValueArrayInt16:
		return v.Nulls
	case QValueArrayInt32:
		return v.Nulls
	case QValueArrayInt64:
		return v.Nulls
	case QValueArrayString:
		return v.Nulls
	case QValueArrayDate:
		return v.Nulls
	case QValueArrayTime:
		return v.Nulls
	case QValueArrayTimestamp:
		return v.Nulls
	case QValueArrayTimestampTZ:
		return v.Nulls
	case QValueArrayBoolean:
		return v.Nulls
	case QValueArrayEnum:
		return v.Nulls
	case QValueArrayNumeric:
		return v.Nulls
	}
	return nil
}

// CheckArrayNulls returns an error if qv's NULL-element mask doesn't have one entry per element. WithArrayNulls never
// builds such a value; writers check before indexing the mask, so a bug elsewhere fails the batch instead of panicking.
func CheckArrayNulls(qv QValue) error {
	nulls := ArrayNulls(qv)
	if nulls == nil {
		return nil
	}
	if rv := reflect.ValueOf(qv.Value()); rv.Kind() != reflect.Slice || rv.Len() != len(nulls) {
		return fmt.Errorf("%s value has a NULL-element mask of %d entries for %v", qv.Kind(), len(nulls), qv.Value())
	}
	return nil
}

// WithArrayNulls returns qv with its NULL-element mask set. A mask without any NULL is dropped (nil), and values
// that aren't arrays, or whose length doesn't match the mask, are returned unchanged.
func WithArrayNulls(qv QValue, nulls []bool) QValue {
	hasNull := false
	for _, isNull := range nulls {
		hasNull = hasNull || isNull
	}
	if !hasNull {
		return qv
	}
	switch v := qv.(type) {
	case QValueArrayInterval:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayUUID:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayFloat32:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayFloat64:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayInt16:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayInt32:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayInt64:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayString:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayDate:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayTime:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayTimestamp:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayTimestampTZ:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayBoolean:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayEnum:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	case QValueArrayNumeric:
		if len(v.Val) == len(nulls) {
			v.Nulls = nulls
			return v
		}
	}
	return qv
}
