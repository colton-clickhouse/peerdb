package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// constrained geometry / geography columns (GEOMETRY(POINT,4326)) were not
// recognized, so the snapshot copied them as text holding hex EWKB and the first CDC write failed to decode the
// changefeed's GeoJSON, stopping CDC for the mirror. They should be created with the same constraint on Postgres
// and keep their SRID on both paths: the snapshot dropped it in COPY, and changefeed GeoJSON has none, but a
// constrained column's type says what it is. Row 1 comes from the snapshot, 2 from a CDC insert, then 1 is updated.
func (s CRDBPGSuite) Test_Constrained_Geo_Snapshot_And_CDC() {
	t := s.t
	srcTable := "constrained_geo"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const (
		row1 = `'SRID=4326;POINT(1 2)', 'POLYGON((0 0,1 0,1 1,0 0))', 'SRID=4326;POINT(3 4)',
			'SRID=3857;POINT Z (1 2 3)', 'SRID=3857;LINESTRING(0 0,1 1)', 'SRID=4326;LINESTRING(0 0,1 1)'`
		row2 = `'SRID=4326;POINT(5 6)', 'POLYGON((0 0,2 0,2 2,0 0))', 'SRID=4326;POINT(7 8)',
			'SRID=3857;POINT Z (4 5 6)', 'SRID=3857;POINT(9 9)', 'SRID=4326;LINESTRING(1 1,2 2)'`
		want1 = `SRID=4326;POINT(1 2)|POLYGON((0 0,1 0,1 1,0 0))|SRID=4326;POINT(3 4)|SRID=3857;POINT(1 2 3)|` +
			`SRID=3857;LINESTRING(0 0,1 1)|SRID=4326;LINESTRING(0 0,1 1)`
		want2 = `SRID=4326;POINT(5 6)|POLYGON((0 0,2 0,2 2,0 0))|SRID=4326;POINT(7 8)|SRID=3857;POINT(4 5 6)|` +
			`SRID=3857;POINT(9 9)|SRID=4326;LINESTRING(1 1,2 2)`
	)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v INT8,
		p GEOMETRY(POINT,4326), poly GEOMETRY(POLYGON), gp GEOGRAPHY(POINT,4326), pz GEOMETRY(POINTZ,3857),
		anyg GEOMETRY(GEOMETRY,3857), gl GEOGRAPHY(LINESTRING))`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 0, %s)`, srcFull, row1)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 0, %s)`, srcFull, row2)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 1 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 OR (id = 1 AND v = 1)`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, "geometry(Point,4326)|geometry(Polygon)|geography(Point,4326)|geometry(PointZ,3857)|"+
		"geometry(Geometry,3857)|geography(LineString,4326)", s.destText(fmt.Sprintf(
		`SELECT string_agg(format_type(atttypid, atttypmod), '|' ORDER BY attnum) FROM pg_attribute
		WHERE attrelid = '%s'::regclass AND attname IN ('p', 'poly', 'gp', 'pz', 'anyg', 'gl')`, dstFull)))
	for id, want := range map[int]string{1: want1, 2: want2} {
		require.Equal(t, want, s.destText(fmt.Sprintf(`SELECT concat_ws('|', ST_AsEWKT(p), ST_AsEWKT(poly), ST_AsEWKT(gp),
			ST_AsEWKT(pz), ST_AsEWKT(anyg), ST_AsEWKT(gl)) FROM %s WHERE id = %d`, dstFull, id)), "id %d", id)
	}
}

// Unconstrained geometry / geography values with an SRID: changefeed GeoJSON carries none, so CDC writes geometry
// without an SRID and geography as 4326. The snapshot must write the same, or one column holds values in several SRIDs
// (which PostGIS refuses to combine) and a CDC update changes a snapshot row's SRID. Row 1 is checked as the snapshot
// wrote it, before any CDC, then after a CDC update; row 2 comes from a CDC insert.
func (s CRDBPGSuite) Test_Unconstrained_Geo_SRID_Same_On_Both_Paths() {
	t := s.t
	srcTable := "unconstrained_geo"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	const values = `'SRID=4326;POINT(1 2)', 'SRID=4269;POINT(1 2)'`
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT8 PRIMARY KEY, v INT8, g GEOMETRY, gg GEOGRAPHY)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, 0, %s)`, srcFull, values)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	srids := func(id int) string {
		return s.destText(fmt.Sprintf(`SELECT concat_ws('|', ST_SRID(g), ST_SRID(gg::geometry), ST_AsText(g), ST_AsText(gg))
			FROM %s WHERE id = %d`, dstFull, id))
	}
	const want = "0|4326|POINT(1 2)|POINT(1 2)"
	EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
	})
	require.Equal(t, want, srids(1), "snapshot row")
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, 0, %s)`, srcFull, values)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET v = 1 WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2 OR (id = 1 AND v = 1)`, dstFull)) == 2
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Equal(t, want, srids(1), "row 1 after a CDC update")
	require.Equal(t, want, srids(2), "CDC insert")
}
