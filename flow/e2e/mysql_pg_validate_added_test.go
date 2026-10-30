package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Validating a resync reads the source schema, and refuses only what would break the recreated tables: a lone
// 64-byte column (truncated consistently) is accepted, two columns colliding once truncated are refused.
func (s MySQLPGSuite) Test_Resync_Validation_Long_Identifiers() {
	t := s.t
	lone, colliding := "resync_long_lone", "resync_long_collide"
	colA, colB := strings.Repeat("c", 63)+"a", strings.Repeat("c", 63)+"b"
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, `%s` INT)", AttachSchema(s, lone), colA)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, `%s` INT, `%s` INT)", AttachSchema(s, colliding), colA, colB)))
	client, err := NewApiClient()
	require.NoError(t, err)
	validateResync := func(table string) error {
		cfg := (&FlowConnectionGenerationConfig{
			FlowJobName:   AddSuffix(s, table),
			TableMappings: TableMappings(s, table, table),
			Destination:   s.Peer().Name,
		}).GenerateFlowConnectionConfigs(s)
		cfg.Resync, cfg.DoInitialSnapshot = true, true
		_, err := client.ValidateCDCMirror(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: cfg})
		return err
	}
	require.NoError(t, validateResync(lone), "a lone long column doesn't block a resync")
	require.ErrorContains(t, validateResync(colliding), colA)
}

// Adding a keyless table to a paused mirror is refused, naming it, and the mirror stays paused
func (s MySQLPGSuite) Test_Add_Keyless_Table_Refused() {
	t := s.t
	keyed, keyless := "add_tbl_keyed", "add_tbl_keyless"
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY)`, AttachSchema(s, keyed))))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT)`, AttachSchema(s, keyless))))
	cfg := (&FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, keyed),
		TableMappings: TableMappings(s, keyed, keyed),
		Destination:   s.Peer().Name,
	}).GenerateFlowConnectionConfigs(s)
	env := ExecutePeerflow(t, NewTemporalClient(t), cfg)
	SetupCDCFlowStatusQuery(t, env, cfg)
	client, err := NewApiClient()
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
	EnvWaitFor(t, env, 2*time.Minute, "paused", func() bool { return status() == protos.FlowStatus_STATUS_PAUSED })
	_, err = client.FlowStateChange(t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        cfg.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RUNNING,
		FlowConfigUpdate: &protos.FlowConfigUpdate{Update: &protos.FlowConfigUpdate_CdcFlowConfigUpdate{
			CdcFlowConfigUpdate: &protos.CDCFlowConfigUpdate{AdditionalTables: TableMappings(s, keyless, keyless)},
		}},
	})
	require.ErrorContains(t, err, "tables without a primary key can't be replicated with CDC to Postgres: "+
		AttachSchema(s, keyless))
	require.Equal(t, protos.FlowStatus_STATUS_PAUSED, status(), "the refused update leaves the mirror paused")
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
