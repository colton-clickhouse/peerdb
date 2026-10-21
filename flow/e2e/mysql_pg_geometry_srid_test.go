package e2e

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

// MySQL geometry values carry an SRID (spatial reference system), which must arrive on Postgres (PostGIS)
// with the value, through the snapshot (rows 1-3) and CDC (rows 4 and 2), including different SRIDs in one
// column. SRID 0 (none) stays 0.
func (s MySQLPGSuite) Test_Geometry_SRID_Kept() {
	t := s.t
	srcTable := "geometry_srid"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	_, err := s.dest.Conn().Exec(t.Context(), "CREATE EXTENSION IF NOT EXISTS postgis")
	require.NoError(t, err)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, g GEOMETRY)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, ST_GeomFromText('POINT(1 2)')),
		(2, ST_GeomFromText('POINT(10 20)', 4326, 'axis-order=long-lat')),
		(3, ST_GeomFromText('POINT(1000 2000)', 3857))`, srcFull)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 3
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (4, ST_GeomFromText('POINT(30 40)', 4326, 'axis-order=long-lat'))`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET g = ST_GeomFromText('POINT(3000 4000)', 3857) WHERE id = 2`, srcFull)))
	EnvWaitFor(t, env, 2*time.Minute, "cdc insert and update", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 4 OR ST_X(g) = 3000`, dstFull)) == 2
	})

	rows, err := s.dest.Conn().Query(t.Context(), fmt.Sprintf(`SELECT id, ST_AsEWKT(g) FROM %s ORDER BY id`, dstFull))
	EnvNoError(t, env, err)
	got := map[int32]string{}
	for rows.Next() {
		var id int32
		var g string
		EnvNoError(t, env, rows.Scan(&id, &g))
		got[id] = g
	}
	EnvNoError(t, env, rows.Err())
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	require.Equal(t, map[int32]string{
		1: "POINT(1 2)",
		2: "SRID=3857;POINT(3000 4000)",
		3: "SRID=3857;POINT(1000 2000)",
		4: "SRID=4326;POINT(30 40)",
	}, got)
}
