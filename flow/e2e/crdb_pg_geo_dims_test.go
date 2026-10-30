package e2e

import (
	"fmt"
	"strconv"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Geometries with an M dimension. Changefeed GeoJSON has no M: XYZM arrives as 4 coordinates, which GEOS refused, so
// CDC wrote NULL for them without a word, and XYM as 3, which read as XYZ. A column whose type has M reads them as
// XYM now. Row 1 comes from the snapshot and is checked before any CDC touches it, 2 from a CDC insert, then 1 is
// updated; all must hold the source's values.
func (s CRDBPGSuite) Test_Geo_M_Dimensions_Snapshot_And_CDC() {
	t := s.t
	srcTable := "geo_dims"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const values = `'POINT ZM (1 2 3 4)', 'SRID=4326;POINT M (1 2 3)', 'LINESTRING ZM (0 0 1 5, 1 1 2 6)',
		'SRID=4326;LINESTRING M (0 0 1, 1 1 2)'`
	const want = "0:POINT ZM (1 2 3 4)|4326:POINT M (1 2 3)|0:LINESTRING ZM (0 0 1 5,1 1 2 6)|" +
		"4326:LINESTRING M (0 0 1,1 1 2)"

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v INT8,
		g GEOMETRY, pm GEOMETRY(POINTM,4326), lzm GEOMETRY(LINESTRINGZM), lm GEOGRAPHY(LINESTRINGM,4326))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 0, %s)`, srcFull, values)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	row := func(id int) string {
		return s.destText(fmt.Sprintf(`SELECT concat_ws('|', ST_SRID(g) || ':' || ST_AsText(g),
			ST_SRID(pm) || ':' || ST_AsText(pm), ST_SRID(lzm) || ':' || ST_AsText(lzm),
			ST_SRID(lm::geometry) || ':' || ST_AsText(lm)) FROM %s WHERE id = %d`, dstFull, id))
	}
	snapshotRow := row(1)
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 0, %s)`, srcFull, values)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 1 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 OR (id = 1 AND v = 1)`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, want, snapshotRow, "snapshot row")
	require.Equal(t, want, row(2), "CDC insert")
	require.Equal(t, want, row(1), "CDC update")
}

// Mirrors from before InternalVersion_CockroachGeoDimensions keep the older reading: an XYZM value written through CDC
// becomes NULL (the snapshot keeps it).
func (s CRDBPGSuite) Test_Geo_ZM_Old_Version_Writes_Null() {
	t := s.t
	srcTable := "geo_dims_old"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, g GEOMETRY)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 'POINT ZM (1 2 3 4)')`, srcFull)))

	flowConnConfig := s.startMirror(srcTable)
	version := shared.InternalVersion_CockroachGeoDimensions - 1
	// mirror creation replaces Version with PEERDB_FORCE_INTERNAL_VERSION (default: latest)
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(version), 10)}
	flowConnConfig.Version = version
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 'POINT ZM (1 2 3 4)')`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Equal(t, "1:POINT ZM (1 2 3 4)|2:NULL", s.destText(fmt.Sprintf(
		`SELECT string_agg(id || ':' || coalesce(ST_AsText(g), 'NULL'), '|' ORDER BY id) FROM %s`, dstFull)))
}
