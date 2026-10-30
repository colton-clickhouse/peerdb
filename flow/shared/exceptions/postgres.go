package exceptions

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// PostgresSetupError represents errors during setup of Postgres peers, maybe we can later replace with a more generic error type
type PostgresSetupError struct {
	error
}

func NewPostgresSetupError(err error) *PostgresSetupError {
	return &PostgresSetupError{err}
}

func (e *PostgresSetupError) Error() string {
	return "Postgres setup error: " + e.error.Error()
}

func (e *PostgresSetupError) Unwrap() error {
	return e.error
}

type CatalogError struct {
	error
}

func NewCatalogError(err error) *CatalogError {
	return &CatalogError{err}
}

func (e *CatalogError) Unwrap() error {
	return e.error
}

type PostgresWalError struct {
	error
	Msg *pgproto3.ErrorResponse
}

func NewPostgresWalError(err error, msg *pgproto3.ErrorResponse) *PostgresWalError {
	return &PostgresWalError{err, msg}
}

func (e *PostgresWalError) Unwrap() error {
	return e.error
}

func (e *PostgresWalError) Error() string {
	return fmt.Sprintf("Postgres WAL error: %s, message: %+v", e.error.Error(), e.Msg)
}

func (e *PostgresWalError) UnderlyingError() *pgproto3.ErrorResponse {
	return e.Msg
}

type TablesNotInPublicationError struct {
	publicationName string
	tables          []string
}

func NewTablesNotInPublicationError(tables []string, publicationName string) *TablesNotInPublicationError {
	return &TablesNotInPublicationError{publicationName, tables}
}

func (e *TablesNotInPublicationError) Error() string {
	return fmt.Sprintf("some additional tables not present in user-provided publication %s: %s",
		e.publicationName,
		strings.Join(e.tables, ","))
}

type PostgresLogicalMessageProcessingError struct {
	error
}

func NewPostgresLogicalMessageProcessingError(err error) *PostgresLogicalMessageProcessingError {
	return &PostgresLogicalMessageProcessingError{err}
}

func (e *PostgresLogicalMessageProcessingError) Error() string {
	return "Logical message processing error: " + e.error.Error()
}

func (e *PostgresLogicalMessageProcessingError) Unwrap() error {
	return e.error
}

type PublicationMissingError struct {
	PublicationName string
}

func NewPublicationMissingError(publicationName string) *PublicationMissingError {
	return &PublicationMissingError{PublicationName: publicationName}
}

func (e *PublicationMissingError) Error() string {
	return fmt.Sprintf("publication %s does not exist", e.PublicationName)
}

type SlotMissingError struct {
	SlotName string
}

func NewSlotMissingError(slotName string) *SlotMissingError {
	return &SlotMissingError{SlotName: slotName}
}

func (e *SlotMissingError) Error() string {
	return fmt.Sprintf("replication slot %s does not exist", e.SlotName)
}

type ReplStateDesyncError struct {
	Message string
}

func NewReplStateDesyncError(message string) *ReplStateDesyncError {
	return &ReplStateDesyncError{Message: message}
}

func (e *ReplStateDesyncError) Error() string {
	return e.Message
}

// SharedRawTableError: two mirrors into one Postgres database whose raw table names Postgres truncates to the same
// name, so they share one raw table
type SharedRawTableError struct {
	Mirror      string
	OtherMirror string
	RawTable    string
}

func NewSharedRawTableError(mirror string, otherMirror string, rawTable string) *SharedRawTableError {
	return &SharedRawTableError{Mirror: mirror, OtherMirror: otherMirror, RawTable: rawTable}
}

func (e *SharedRawTableError) Error() string {
	return fmt.Sprintf("mirrors %s and %s write to the same database and share the raw table %s, since Postgres "+
		"truncates their names to 63 bytes: one's cleanup deletes the other's unnormalized rows; recreate one of them "+
		"with a shorter name", e.Mirror, e.OtherMirror, e.RawTable)
}
