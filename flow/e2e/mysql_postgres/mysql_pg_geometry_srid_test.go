//go:build tilt

package mysql_postgres

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
)

// MySQL geometry values carry an SRID (spatial reference system), which must arrive on Postgres (PostGIS)
// with the value, through the snapshot (rows 1-3) and CDC (rows 4 and 2), including different SRIDs in one
// column. SRID 0 (none) stays 0.
func (s MySQLPGSuite) Test_Geometry_SRID_Kept() {
	t := s.t
	srcTable := "geometry_srid"
	srcFull := e2e.AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)
	_, err := s.dest.Conn().Exec(t.Context(), "CREATE EXTENSION IF NOT EXISTS postgis")
	require.NoError(t, err)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, g GEOMETRY)`, srcFull)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES
		(1, ST_GeomFromText('POINT(1 2)')),
		(2, ST_GeomFromText('POINT(10 20)', 4326, 'axis-order=long-lat')),
		(3, ST_GeomFromText('POINT(1000 2000)', 3857))`, srcFull)))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, srcTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "initial load", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 3
	})
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (4, ST_GeomFromText('POINT(30 40)', 4326, 'axis-order=long-lat'))`, srcFull)))
	e2e.EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET g = ST_GeomFromText('POINT(3000 4000)', 3857) WHERE id = 2`, srcFull)))
	e2e.EnvWaitFor(t, env, 2*time.Minute, "cdc insert and update", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 4 OR ST_X(g) = 3000`, dstFull)) == 2
	})

	rows, err := s.dest.Conn().Query(t.Context(), fmt.Sprintf(`SELECT id, ST_AsEWKT(g) FROM %s ORDER BY id`, dstFull))
	e2e.EnvNoError(t, env, err)
	got := map[int32]string{}
	for rows.Next() {
		var id int32
		var g string
		e2e.EnvNoError(t, env, rows.Scan(&id, &g))
		got[id] = g
	}
	e2e.EnvNoError(t, env, rows.Err())
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)

	require.Equal(t, map[int32]string{
		1: "POINT(1 2)",
		2: "SRID=3857;POINT(3000 4000)",
		3: "SRID=3857;POINT(1000 2000)",
		4: "SRID=4326;POINT(30 40)",
	}, got)
}
