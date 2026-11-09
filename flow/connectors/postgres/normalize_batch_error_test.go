package connpostgres

import (
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
)

func TestNormalizeBatchError(t *testing.T) {
	c := &PostgresConnector{logger: log.NewStructuredLogger(slog.Default())}
	entries := []batchEntry{{tableName: "s.a", statement: "MERGE INTO s.a"}, {tableName: "s.b", statement: "MERGE INTO s.b"}}

	// a statement failing as it runs is reported under its own table
	err := c.normalizeBatchError(errors.New("boom"), 1, entries[1], entries)
	require.ErrorContains(t, err, "normalize statement for table s.b")

	// a statement that fails to prepare comes back with the first result. pgx has no constructor for the error;
	// the zero value's SQL matches no table's statement, as the trailing metadata update's doesn't
	err = c.normalizeBatchError(fmt.Errorf("wrapped: %w", pgx.ErrPreprocessingBatch{}), 1, entries[0], entries)
	require.ErrorContains(t, err, "failed to update metadata for NormalizeTables")
	require.NotContains(t, err.Error(), "s.a", "the metadata update's failure must not name the first table")
	var preprocessingErr pgx.ErrPreprocessingBatch
	require.ErrorAs(t, err, &preprocessingErr)
}
