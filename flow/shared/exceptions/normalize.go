package exceptions

type NormalizationError struct {
	error
}

func NewNormalizationError(err error) *NormalizationError {
	return &NormalizationError{err}
}

func (e *NormalizationError) Error() string {
	return "Normalization Error: " + e.error.Error()
}

func (e *NormalizationError) Unwrap() error {
	return e.error
}

// PostgresNormalizeTableError: a Postgres destination's normalize statement for one table failed
type PostgresNormalizeTableError struct {
	error
	TableName string
}

func NewPostgresNormalizeTableError(tableName string, err error) *PostgresNormalizeTableError {
	return &PostgresNormalizeTableError{error: err, TableName: tableName}
}

func (e *PostgresNormalizeTableError) Error() string {
	return "error executing normalize statement for table " + e.TableName + ": " + e.error.Error()
}

func (e *PostgresNormalizeTableError) Unwrap() error {
	return e.error
}
