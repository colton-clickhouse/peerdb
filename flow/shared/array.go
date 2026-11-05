package shared

import "github.com/PeerDB-io/peerdb/flow/shared/exceptions"

// first - second
func ArrayMinus[T comparable](first, second []T) []T {
	lookup := make(map[T]struct{}, len(second))
	// Add elements from arrayB to the lookup map
	for _, element := range second {
		lookup[element] = struct{}{}
	}
	// Iterate over arrayA and check if the element is present in the lookup map
	var result []T
	for _, element := range first {
		_, exists := lookup[element]
		if !exists {
			result = append(result, element)
		}
	}
	return result
}

// ArrayCastElements converts decoded array elements to T. A nil (NULL) element becomes T's zero value; callers
// that keep NULL elements mark them separately (types.WithArrayNulls). Any other element that isn't a T is an
// error (exceptions.ArrayElementTypeError): converting it would silently write a zero value.
func ArrayCastElements[T any](arr []any) ([]T, error) {
	res := make([]T, 0, len(arr))
	for i, val := range arr {
		if val == nil {
			res = append(res, *new(T))
			continue
		}
		v, ok := val.(T)
		if !ok {
			return nil, exceptions.NewArrayElementTypeError(i, val, *new(T))
		}
		res = append(res, v)
	}
	return res, nil
}
