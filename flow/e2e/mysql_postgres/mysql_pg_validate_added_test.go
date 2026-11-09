//go:build tilt

package mysql_postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	e2e "github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Validating a resync reads the source schema, and refuses only what would break the recreated tables: a lone
// 64-byte column (truncated consistently) is accepted, two columns colliding once truncated are refused.
func (s MySQLPGSuite) Test_Resync_Validation_Long_Identifiers() {
	t := s.t
	lone, colliding := "resync_long_lone", "resync_long_collide"
	colA, colB := strings.Repeat("c", 63)+"a", strings.Repeat("c", 63)+"b"
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, `%s` INT)", e2e.AttachSchema(s, lone), colA)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, `%s` INT, `%s` INT)", e2e.AttachSchema(s, colliding), colA, colB)))
	client, err := e2e.NewApiClient()
	require.NoError(t, err)
	validateResync := func(table string) error {
		cfg := (&e2e.FlowConnectionGenerationConfig{
			FlowJobName:   e2e.AddSuffix(s, table),
			TableMappings: e2e.TableMappings(s, table, table),
			Destination:   s.Peer().Name,
		}).GenerateFlowConnectionConfigs(s)
		cfg.Resync, cfg.DoInitialSnapshot = true, true
		_, err := client.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: cfg})
		return err
	}
	require.NoError(t, validateResync(lone), "a lone long column doesn't block a resync")
	require.ErrorContains(t, validateResync(colliding), colA)
}

// Adding a keyless table to a paused mirror is refused, naming it, and the mirror stays paused. Adding a table whose
// destination table already holds rows (a removed table added back) is accepted, as it was before tables added to a
// mirror were validated.
func (s MySQLPGSuite) Test_Add_Keyless_Table_Refused() {
	t := s.t
	keyed, keyless := "add_tbl_keyed", "add_tbl_keyless"
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY)`, e2e.AttachSchema(s, keyed))))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT)`, e2e.AttachSchema(s, keyless))))
	cfg := (&e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, keyed),
		TableMappings: e2e.TableMappings(s, keyed, keyed),
		Destination:   s.Peer().Name,
	}).GenerateFlowConnectionConfigs(s)
	env := e2e.ExecutePeerflow(t, e2e.NewTemporalClient(t), cfg)
	e2e.SetupCDCFlowStatusQuery(t, env, cfg)
	client, err := e2e.NewApiClient()
	require.NoError(t, err)
	status := func() protos.FlowStatus {
		res, err := client.MirrorStatus(t.Context(), &protos.MirrorStatusRequest{FlowJobName: cfg.FlowJobName})
		if err != nil {
			return protos.FlowStatus_STATUS_UNKNOWN
		}
		return res.CurrentFlowState
	}
	_, err = client.FlowStateChange(t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName: cfg.FlowJobName, RequestedFlowState: protos.FlowStatus_STATUS_PAUSED,
	})
	require.NoError(t, err)
	e2e.EnvWaitFor(t, env, 2*time.Minute, "paused", func() bool { return status() == protos.FlowStatus_STATUS_PAUSED })
	_, err = client.FlowStateChange(t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        cfg.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
			CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{AdditionalTables: e2e.TableMappings(s, keyless, keyless)},
		}},
	})
	require.ErrorContains(t, err, "tables without a primary key can't be replicated with CDC to Postgres: "+
		e2e.AttachSchema(s, keyless))
	require.Equal(t, protos.FlowStatus_STATUS_PAUSED, status(), "the refused update leaves the mirror paused")

	// a MySQL source leaves excluded columns out of the key it reports: the table's own key is checked
	compositeKey := "add_tbl_composite_key"
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (a INT, b_part INT, v INT, PRIMARY KEY (a, b_part))`, e2e.AttachSchema(s, compositeKey))))
	excluded := e2e.TableMappings(s, compositeKey, compositeKey)
	excluded[0].Exclude = []string{"b_part"}
	_, err = client.FlowStateChange(t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        cfg.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
			CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{AdditionalTables: excluded},
		}},
	})
	require.ErrorContains(t, err, "primary key columns of "+e2e.AttachSchema(s, compositeKey)+
		" can't be excluded from a CDC mirror to Postgres: b_part")

	prefilled := "add_tbl_prefilled"
	require.NoError(t, s.source.Exec(t.Context(),
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY)`, e2e.AttachSchema(s, prefilled))))
	_, err = s.dest.Conn().Exec(t.Context(), fmt.Sprintf(
		`CREATE TABLE %[1]s (id INT PRIMARY KEY); INSERT INTO %[1]s VALUES (100)`, s.DestinationTable(prefilled)))
	require.NoError(t, err)
	_, err = client.FlowStateChange(t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        cfg.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
			CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{AdditionalTables: e2e.TableMappings(s, prefilled, prefilled)},
		}},
	})
	require.NoError(t, err, "a table with rows on the destination can be added")
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
