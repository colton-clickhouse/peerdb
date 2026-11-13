package e2e

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// dynamicSettingHolds counts, per setting, the tests that currently hold it changed, with the value it had before
// the first of them: suites run their tests in parallel, and a test that saved another test's value as "previous"
// would put it back after both finished.
var (
	dynamicSettingMu    sync.Mutex
	dynamicSettingHolds = map[string]*dynamicSettingHold{}
)

type dynamicSettingHold struct {
	original string
	count    int
}

// SetDynamicSetting sets a dynamic setting for the test and puts its original value back once no test holds it any
// more (the API can't unset one). Settings are global for the stack: parallel tests see the value, and when they
// set the same setting, the last one set wins until all of them are done.
func SetDynamicSetting(t *testing.T, name string, value string) {
	t.Helper()
	client, err := NewApiClient()
	require.NoError(t, err)
	set := func(ctx context.Context, v string) error {
		_, err := client.PostDynamicSetting(ctx, &protos.PostDynamicSettingRequest{Name: name, Value: proto.String(v)})
		return err
	}

	dynamicSettingMu.Lock()
	defer dynamicSettingMu.Unlock()
	hold, ok := dynamicSettingHolds[name]
	if !ok {
		settings, err := client.GetDynamicSettings(t.Context(), &protos.GetDynamicSettingsRequest{})
		require.NoError(t, err)
		original := ""
		for _, ds := range settings.Settings {
			if ds.Name == name {
				original = ds.GetValue()
				if original == "" {
					original = ds.DefaultValue
				}
			}
		}
		require.NotEmpty(t, original, name)
		hold = &dynamicSettingHold{original: original}
		dynamicSettingHolds[name] = hold
	}
	require.NoError(t, set(t.Context(), value))
	hold.count++
	// t.Context() is already canceled when cleanups run
	t.Cleanup(func() {
		dynamicSettingMu.Lock()
		defer dynamicSettingMu.Unlock()
		hold.count--
		if hold.count == 0 {
			delete(dynamicSettingHolds, name)
			require.NoError(t, set(context.Background(), hold.original))
		}
	})
}
