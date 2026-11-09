//go:build tilt

package postgres_postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
)

// Postgres sources: a REPLICA IDENTITY FULL table with a primary key gets that key on the target, as one with the
// default replica identity does. A keyless REPLICA IDENTITY FULL table, which PeerDB keys on all its columns, gets
// none (its rows needn't be unique). Updates and deletes of the keyed table still apply.
func (s PeerFlowE2ETestSuitePG) Test_Replica_Identity_Full_Keeps_Primary_Key() {
	tc := e2e.NewTemporalClient(s.t)
	keyed, keyedDst := s.attachSchemaSuffix("rif_full_keyed"), s.attachSchemaSuffix("rif_full_keyed_dst")
	keyless, keylessDst := s.attachSchemaSuffix("rif_full_keyless"), s.attachSchemaSuffix("rif_full_keyless_dst")
	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE %[1]s (id int, k text, v text, PRIMARY KEY (id, k));
		ALTER TABLE %[1]s REPLICA IDENTITY FULL;
		INSERT INTO %[1]s VALUES (1, 'a', 'one'), (2, 'b', 'two');
		CREATE TABLE %[2]s (a int, b text);
		ALTER TABLE %[2]s REPLICA IDENTITY FULL;
		INSERT INTO %[2]s VALUES (1, 'x'), (1, 'x');`, keyed, keyless))
	require.NoError(s.t, err)

	flowConnConfig := (&e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("rif_full_pkey"),
		TableNameMapping: map[string]string{keyed: keyedDst, keyless: keylessDst},
		Destination:      s.Peer().Name,
	}).GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	// the suite's source and destination are one database, whose publication is FOR ALL TABLES: an update of a
	// target table without a replica identity fails, and the snapshot updates the synced-at column
	flowConnConfig.SyncedAtColName = ""
	env := e2e.ExecutePeerflow(s.t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "snapshot", func() bool {
		return s.comparePGTables(keyed, keyedDst, "id,k,v") == nil && s.comparePGTables(keyless, keylessDst, "a,b") == nil
	})

	primaryKey := func(table string) string {
		var def string
		if err := s.Conn().QueryRow(s.t.Context(),
			`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'p'`, table,
		).Scan(&def); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			e2e.EnvNoError(s.t, env, err)
		}
		return def
	}
	// checked before CDC: without the key, CDC's updates of the keyed target would fail on that publication too
	const wantKey = "PRIMARY KEY (id, k)"
	if keyedKey, keylessKey := primaryKey(keyedDst), primaryKey(keylessDst); keyedKey != wantKey || keylessKey != "" {
		env.Cancel(s.t.Context())
		e2e.RequireEnvCanceled(s.t, env)
		require.Equal(s.t, wantKey, keyedKey, "the keyed REPLICA IDENTITY FULL table's key on the target")
		require.Empty(s.t, keylessKey, "a keyless REPLICA IDENTITY FULL table gets no key on the target")
	}

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		UPDATE %[1]s SET v = 'uno' WHERE id = 1;
		DELETE FROM %[1]s WHERE id = 2;
		INSERT INTO %[1]s VALUES (3, 'c', 'three');`, keyed))
	e2e.EnvNoError(s.t, env, err)
	e2e.EnvWaitFor(s.t, env, 2*time.Minute, "CDC on the keyed table", func() bool {
		return s.comparePGTables(keyed, keyedDst, "id,k,v") == nil
	})
	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}
