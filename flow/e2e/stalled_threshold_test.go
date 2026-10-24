package e2e

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const stalledThresholdSetting = "PEERDB_STALLED_THRESHOLD_MINUTES"

// stalledThreshold is shared by the tests that lower PEERDB_STALLED_THRESHOLD_MINUTES: the setting is global for the
// stack and the suites run in parallel, so the first test lowers it and the last one to finish restores it (the API
// can't unset it, so it gets its previous value back).
var stalledThreshold struct {
	sync.Mutex
	users    int
	previous string
}

func setStalledThreshold(ctx context.Context, client protos.FlowServiceClient, minutes string) error {
	_, err := client.PostDynamicSetting(ctx, &protos.PostDynamicSettingRequest{
		Name: stalledThresholdSetting, Value: proto.String(minutes),
	})
	return err
}

// lowerStalledThreshold sets the STALLED threshold to 1 minute until t and every other test using it are done.
func lowerStalledThreshold(t *testing.T, client protos.FlowServiceClient) {
	t.Helper()
	stalledThreshold.Lock()
	defer stalledThreshold.Unlock()
	if stalledThreshold.users == 0 {
		settings, err := client.GetDynamicSettings(t.Context(), &protos.GetDynamicSettingsRequest{})
		require.NoError(t, err)
		previous := ""
		for _, ds := range settings.Settings {
			if ds.Name == stalledThresholdSetting {
				previous = ds.GetValue()
				if previous == "" {
					previous = ds.DefaultValue
				}
			}
		}
		require.NotEmpty(t, previous, stalledThresholdSetting)
		require.NoError(t, setStalledThreshold(t.Context(), client, "1"))
		stalledThreshold.previous = previous
	}
	stalledThreshold.users++
	t.Cleanup(func() {
		stalledThreshold.Lock()
		defer stalledThreshold.Unlock()
		stalledThreshold.users--
		if stalledThreshold.users == 0 {
			// t.Context() is already canceled when cleanups run
			require.NoError(t, setStalledThreshold(context.Background(), client, stalledThreshold.previous))
		}
	})
}
