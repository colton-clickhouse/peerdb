package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

// Geometries with an M dimension. Changefeed GeoJSON has no M: XYZM arrives as 4 coordinates, which GEOS refuses, so
// CDC used to write NULL for them without a word, and XYM as 3, which reads as XYZ. The snapshot (EWKB) keeps both;
// CDC now fails on them with a classified error instead of changing the value.
func (s CRDBPGSuite) Test_Geo_M_Dimensions_Fail_CDC() {
	for _, tc := range []struct {
		table, column, value, logged string
	}{
		{"geo_zm", "g GEOMETRY", "'POINT ZM (1 2 3 4)'", "GeoJSON"},
		{"geo_m", "g GEOMETRY(POINTM,4326)", "'SRID=4326;POINT M (1 2 3)'", "has an M dimension"},
	} {
		s.t.Run(tc.table, func(t *testing.T) {
			srcFull := AttachSchema(s, tc.table)
			dstFull := s.DestinationTable(tc.table)
			require.NoError(t, s.source.Exec(t.Context(),
				fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, %s)`, srcFull, tc.column)))
			require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, %s)`, srcFull, tc.value)))

			flowConnConfig := s.startMirror(tc.table)
			tc2 := NewTemporalClient(t)
			env := ExecutePeerflow(t, tc2, flowConnConfig)
			SetupCDCFlowStatusQuery(t, env, flowConnConfig)
			EnvWaitFor(t, env, 3*time.Minute, "snapshot row", func() bool {
				return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 1
			})
			require.Equal(t, s.destText(fmt.Sprintf(`SELECT ST_AsText(%s::geometry)`, tc.value)),
				s.destText(fmt.Sprintf(`SELECT ST_AsText(g) FROM %s WHERE id = 1`, dstFull)), "the snapshot keeps M")

			EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (2, %s)`, srcFull, tc.value)))
			catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(t.Context())
			require.NoError(t, err)
			EnvWaitFor(t, env, 3*time.Minute, "CDC fails on the value", func() bool {
				count, err := GetLogCount(t.Context(), catalogPool, flowConnConfig.FlowJobName, "error", tc.logged)
				return err == nil && count > 0
			})
			require.EqualValues(t, 0, s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = 2`, dstFull)),
				"no row with a changed or NULL value")
			env.Cancel(t.Context())
			RequireEnvCanceled(t, env)
		})
	}
}
