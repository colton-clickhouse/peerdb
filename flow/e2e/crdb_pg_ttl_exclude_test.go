package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// Row-level TTL adds a hidden column CockroachDB maintains itself, crdb_internal_expiration. Excluding it in the
// table mapping, the documented way to leave it out, keeps it out of the target through the snapshot and CDC, while
// a keyless table's hidden rowid, which keys the target, stays. Row 1 comes from the snapshot, 2 from a CDC insert,
// then 1 is updated.
func (s CRDBPGSuite) ttlExcludeMirror(srcTable string, keyless bool, ttlMidStream bool) (columns string, rows string) {
	t := s.t
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	key := "id INT8 PRIMARY KEY"
	if keyless {
		key = "id INT8 NOT NULL"
	}
	with := " WITH (ttl_expire_after = '1 day')"
	if ttlMidStream {
		with = ""
	}
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (%s, v STRING)%s`, srcFull, key, with)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (1, 'snapshot')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	flowConnConfig.TableMappings[0].Exclude = []string{"crdb_internal_expiration"}
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	if ttlMidStream {
		EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`ALTER TABLE %s SET (ttl_expire_after = '1 day')`, srcFull)))
	}
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (2, 'cdc')`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 'updated' WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE (id = 1 AND v = 'updated') OR (id = 2 AND v = 'cdc')`,
			dstFull)) == 2 && s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	columns = s.destText(fmt.Sprintf(`SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
		WHERE attrelid = '%s'::regclass AND attnum > 0 AND NOT attisdropped AND attname NOT ILIKE '\_peerdb\_%%'`, dstFull))
	rows = s.destText(fmt.Sprintf(`SELECT string_agg(id || '=' || v, ',' ORDER BY id) FROM %s`, dstFull))
	return columns, rows
}

func (s CRDBPGSuite) Test_TTL_Column_Excluded() {
	columns, rows := s.ttlExcludeMirror("ttl_excl", false, false)
	require.Equal(s.t, "id,v", columns)
	require.Equal(s.t, "1=updated,2=cdc", rows)
}

func (s CRDBPGSuite) Test_TTL_Column_Excluded_Keyless() {
	columns, rows := s.ttlExcludeMirror("ttl_excl_keyless", true, false)
	require.Equal(s.t, "id,v,rowid", columns)
	require.Equal(s.t, "1=updated,2=cdc", rows)
}

func (s CRDBPGSuite) Test_TTL_Column_Excluded_TTL_Added_Mid_Stream() {
	columns, rows := s.ttlExcludeMirror("ttl_excl_added", false, true)
	require.Equal(s.t, "id,v", columns)
	require.Equal(s.t, "1=updated,2=cdc", rows)
}
