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
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// Postgres sources: floats must arrive exactly, in the snapshot, through CDC and as the fill value of an added
// column, whatever the source role's or database's extra_float_digits. pgoutput, COPY text and to_json send them
// in text form, which a setting of 0 or less rounds to 15 (double precision) or 6 (real) significant digits.
func (s PeerFlowE2ETestSuitePG) Test_Float_Precision_Low_Extra_Float_Digits() {
	s.floatPrecision("float_prec_low", true)
}

func (s PeerFlowE2ETestSuitePG) Test_Float_Precision_Default() {
	s.floatPrecision("float_prec_default", false)
}

// lowFloatDigitsPeer is a peer for the test database that connects as its own role, with extra_float_digits = 0
// set on the role: the suite's tests run in parallel and share the catalog role, so that one can't be changed.
func (s PeerFlowE2ETestSuitePG) lowFloatDigitsPeer(name string) *protos.Peer {
	role := "e2e_" + name + "_" + s.suffix
	ctx := s.t.Context()
	for _, stmt := range []string{
		// role is letters, digits and underscores, so it can serve as its own password literal
		fmt.Sprintf(`CREATE ROLE %s LOGIN SUPERUSER PASSWORD '%s'`, common.QuoteIdentifier(role), role),
		fmt.Sprintf(`ALTER ROLE %s SET extra_float_digits = 0`, common.QuoteIdentifier(role)),
	} {
		_, err := s.Conn().Exec(ctx, stmt)
		require.NoError(s.t, err)
	}
	s.t.Cleanup(func() {
		ctx := context.Background()
		_, _ = s.Conn().Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1`, role)
		_, _ = s.Conn().Exec(ctx, fmt.Sprintf(`REASSIGN OWNED BY %s TO current_user`, common.QuoteIdentifier(role)))
		_, _ = s.Conn().Exec(ctx, fmt.Sprintf(`DROP OWNED BY %s`, common.QuoteIdentifier(role)))
		_, _ = s.Conn().Exec(ctx, fmt.Sprintf(`DROP ROLE %s`, common.QuoteIdentifier(role)))
	})

	config := proto.CloneOf(GeneratePostgresPeer(s.t).GetPostgresConfig())
	config.User, config.Password = role, role
	peer := &protos.Peer{
		Name:   role,
		Type:   protos.DBType_POSTGRES,
		Config: &protos.Peer_PostgresConfig{PostgresConfig: config},
	}
	CreatePeer(s.t, peer)
	return peer
}

func (s PeerFlowE2ETestSuitePG) floatPrecision(name string, lowDigits bool) {
	tc := NewTemporalClient(s.t)
	src, dst := s.attachSchemaSuffix(name), s.attachSchemaSuffix(name+"_dst")
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id int PRIMARY KEY, d double precision, r real, da double precision[], ra real[])`, src))
	require.NoError(s.t, err)
	const values = `(0.1::float8 + 0.2::float8, 1/3::real, ARRAY[pi(), 1e-300]::float8[], ARRAY[0.1, 1/3]::real[])`
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s SELECT 1, * FROM (VALUES %s) v`, src, values))
	require.NoError(s.t, err)

	peer := s.Peer()
	if lowDigits {
		peer = s.lowFloatDigitsPeer(name)
		var digits string
		conn, err := pgx.Connect(s.t.Context(), internal.GetPGConnectionString(peer.GetPostgresConfig(), ""))
		require.NoError(s.t, err)
		require.NoError(s.t, conn.QueryRow(s.t.Context(), `SHOW extra_float_digits`).Scan(&digits))
		require.NoError(s.t, conn.Close(s.t.Context()))
		require.Equal(s.t, "0", digits, "the peer's role setting")
	}
	flowConnConfig := (&FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix(name), TableNameMapping: map[string]string{src: dst}, Destination: peer.Name,
	}).GenerateFlowConnectionConfigs(s)
	flowConnConfig.SourceName = peer.Name
	flowConnConfig.DoInitialSnapshot = true
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s SELECT 2, * FROM (VALUES %s) v`, src, values))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 2*time.Minute, "snapshot and CDC rows", func() bool {
		var n int64
		return s.Conn().QueryRow(s.t.Context(), `SELECT count(*) FROM `+dst).Scan(&n) == nil && n == 2
	})
	// rows 1 and 2 get the added columns' fill values, read from the source catalog (attmissingval) in text form
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN fd double precision DEFAULT 0.1::float8 + 0.2::float8, ADD COLUMN fr real DEFAULT 1/3::real`,
		src))
	EnvNoError(s.t, env, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s SELECT 3, * FROM (VALUES %s) v`, src, values))
	EnvNoError(s.t, env, err)
	EnvWaitFor(s.t, env, 2*time.Minute, "added columns and row 3", func() bool {
		var n int64
		return s.Conn().QueryRow(s.t.Context(), `SELECT count(*) FROM `+dst+` WHERE fd IS NOT NULL`).Scan(&n) == nil &&
			n == 3
	})
	var inexact []int32
	rows, err := s.Conn().Query(s.t.Context(), fmt.Sprintf(`SELECT s.id FROM %s s JOIN %s d USING (id)
		WHERE s.d IS DISTINCT FROM d.d OR s.r IS DISTINCT FROM d.r OR s.da IS DISTINCT FROM d.da
			OR s.ra IS DISTINCT FROM d.ra OR s.fd IS DISTINCT FROM d.fd OR s.fr IS DISTINCT FROM d.fr ORDER BY 1`,
		src, dst))
	EnvNoError(s.t, env, err)
	for rows.Next() {
		var id int32
		EnvNoError(s.t, env, rows.Scan(&id))
		inexact = append(inexact, id)
	}
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
	require.Empty(s.t, inexact, "rows whose floats changed (1: snapshot, 2: CDC, 1-3: added columns)")
}
