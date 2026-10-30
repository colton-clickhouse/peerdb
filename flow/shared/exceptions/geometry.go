package exceptions

// GeometryParseError is a geometry or geography value a source sent that PeerDB can't read. Writing NULL instead would
// lose the value without a trace, so it fails the batch; the column (or the mirror) has to change.
type GeometryParseError struct {
	error
}

func NewGeometryParseError(err error) *GeometryParseError {
	return &GeometryParseError{err}
}

func (e *GeometryParseError) Error() string {
	return "failed to read geometry: " + e.error.Error()
}

func (e *GeometryParseError) Unwrap() error {
	return e.error
}
