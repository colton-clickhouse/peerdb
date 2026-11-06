package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// CDC's raw value of a TIMETZ is the UTC time of day without an offset, and normalize casts it through timetz, which
// takes the session's TimeZone. The Postgres connector pins TimeZone=UTC for its sessions, so a destination role
// whose default is another zone still gets +00, on both paths. The role is the test's own: changing the shared role's
// default would reach the suites running alongside.
func (s CRDBPGSuite) Test_TimeTZ_Destination_Role_TimeZone() {
	t := s.t
	srcTable := "timetz_role_tz"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	role := "pmb_tz_" + s.suffix

	_, err := s.dest.Conn().Exec(t.Context(), fmt.Sprintf(
		`CREATE ROLE %[1]s LOGIN SUPERUSER PASSWORD 'pmb_tz'; ALTER ROLE %[1]s SET TimeZone = 'America/New_York'`, role))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = s.dest.Conn().Exec(context.Background(), fmt.Sprintf(`DROP ROLE IF EXISTS %s`, role))
	})
	config := proto.Clone(internal.GetAncillaryPostgresConfigFromEnv()).(*protos.PostgresConfig)
	config.User, config.Password = role, "pmb_tz"
	roleConn, err := pgx.Connect(t.Context(), internal.GetPGConnectionString(config, ""))
	require.NoError(t, err)
	var zone string
	require.NoError(t, roleConn.QueryRow(t.Context(), `SHOW TimeZone`).Scan(&zone))
	require.NoError(t, roleConn.Close(t.Context()))
	require.Equal(t, "America/New_York", zone, "the role's own sessions aren't in UTC")
	peer := &protos.Peer{
		Name: role, Type: protos.DBType_POSTGRES, Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
	}
	CreatePeer(t, peer)

	const values = `'10:00:00+02', '10:00:00+02'`
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, as_timetz TIMETZ, as_text TIMETZ)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, %s)`, srcFull, values)))
	_, err = s.dest.Conn().Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, as_timetz timetz,
		as_text text, "_PEERDB_SYNCED_AT" timestamp DEFAULT CURRENT_TIMESTAMP)`, dstFull))
	require.NoError(t, err)

	flowConnConfig := s.startMirror(srcTable)
	flowConnConfig.DestinationName = peer.Name
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, %s)`, srcFull, values)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	for id, via := range map[int]string{1: "snapshot", 2: "cdc"} {
		// a timetz prints its own stored offset, whatever the reading session's zone
		require.Equal(t, "08:00:00+00|08:00:00+00", s.destText(fmt.Sprintf(
			`SELECT as_timetz::text || '|' || as_text FROM %s WHERE id = %d`, dstFull, id)), via)
	}
}
