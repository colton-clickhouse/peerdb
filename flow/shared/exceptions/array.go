package exceptions

import "fmt"

// ArrayElementTypeError is an array element the source driver decoded as a different Go type than the column's
// kind expects, e.g. INT8[] data under a stored INT4[] kind after the column's type changed. Converting it would
// write a zero value, so the row fails instead. Column is set by the caller that knows the column's name.
type ArrayElementTypeError struct {
	Column string
	Index  int
	Got    string
	Want   string
}

func NewArrayElementTypeError(index int, got any, want any) *ArrayElementTypeError {
	return &ArrayElementTypeError{Index: index, Got: fmt.Sprintf("%T", got), Want: fmt.Sprintf("%T", want)}
}

func (e *ArrayElementTypeError) Error() string {
	column := e.Column
	if column == "" {
		column = "(unknown)"
	}
	return fmt.Sprintf("array column %s: element %d decoded as %s, expected %s; the column's type probably changed"+
		" since the mirror was created: recreate the mirror (or resync it from COMPLETED/FAILED), or exclude the column",
		column, e.Index, e.Got, e.Want)
}

// WithColumn sets the column name if it isn't set yet and returns e.
func (e *ArrayElementTypeError) WithColumn(column string) *ArrayElementTypeError {
	if e.Column == "" {
		e.Column = column
	}
	return e
}
