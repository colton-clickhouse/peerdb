package exceptions

import "fmt"

// ArrayElementTypeError is an array element that doesn't convert to the column's kind, e.g. INT8[] data under a
// stored INT4[] kind after the column's type changed. Converting it would write a zero value, so the row fails
// instead. Column is set by the caller that knows the column's name; Hint says what to do about it.
type ArrayElementTypeError struct {
	Column string
	Index  int
	Got    string
	Want   string
	Hint   string
}

// HintStaleKind is the default hint: the stored schema no longer matches the source column. A resync re-reads it.
const HintStaleKind = "the column's type probably changed since the mirror read its schema: resync the mirror, or" +
	" exclude the column"

// HintUnexpected is for sources whose column kinds come from the values themselves, so a mismatch is a PeerDB bug.
const HintUnexpected = "PeerDB doesn't expect this element type for the column: exclude the column to continue, and" +
	" report it"

func NewArrayElementTypeError(index int, got any, want any) *ArrayElementTypeError {
	return &ArrayElementTypeError{Index: index, Got: fmt.Sprintf("%T", got), Want: fmt.Sprintf("%T", want)}
}

func (e *ArrayElementTypeError) Error() string {
	column := e.Column
	if column == "" {
		column = "(unknown)"
	}
	hint := e.Hint
	if hint == "" {
		hint = HintStaleKind
	}
	return fmt.Sprintf("array column %s: element %d is %s, expected %s; %s", column, e.Index, e.Got, e.Want, hint)
}

// WithColumn sets the column name if it isn't set yet and returns e.
func (e *ArrayElementTypeError) WithColumn(column string) *ArrayElementTypeError {
	if e.Column == "" {
		e.Column = column
	}
	return e
}

// WithHint sets the hint if it isn't set yet and returns e.
func (e *ArrayElementTypeError) WithHint(hint string) *ArrayElementTypeError {
	if e.Hint == "" {
		e.Hint = hint
	}
	return e
}
