package connpostgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// The settings that change values in text form are SET after connecting, not sent as startup parameters, which
// pgbouncer refuses for parameters it doesn't track: on the connector's connection and the replication connection
func TestTextOutputSettingsNotStartupParameters(t *testing.T) {
	pgConfig := &protos.PostgresConfig{Host: "localhost", Port: 5432, User: "u", Password: "p", Database: "d"}
	connectionString := internal.GetPGConnectionString(pgConfig, "")
	conn, err := newConnConfig(connectionString, pgConfig)
	require.NoError(t, err)
	repl, err := newReplConnConfig(connectionString, pgConfig)
	require.NoError(t, err)
	require.Equal(t, "database", repl.Config.RuntimeParams["replication"])
	for name, params := range map[string]map[string]string{
		"connection": conn.Config.RuntimeParams, "replication connection": repl.Config.RuntimeParams,
	} {
		for key := range params {
			for _, setting := range []string{"extra_float_digits", "IntervalStyle", "xmloption"} {
				require.False(t, strings.EqualFold(key, setting), "%s sends %s as a startup parameter", name, key)
			}
		}
	}
}
