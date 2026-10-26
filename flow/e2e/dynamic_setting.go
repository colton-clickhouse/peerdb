package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// SetDynamicSetting sets a dynamic setting for the test and puts its previous value back afterwards (the API
// can't unset one). Settings are global for the stack, so parallel tests see the value too.
func SetDynamicSetting(t *testing.T, name string, value string) {
	t.Helper()
	client, err := NewApiClient()
	require.NoError(t, err)
	settings, err := client.GetDynamicSettings(t.Context(), &protos.GetDynamicSettingsRequest{})
	require.NoError(t, err)
	previous := ""
	for _, ds := range settings.Settings {
		if ds.Name == name {
			previous = ds.GetValue()
			if previous == "" {
				previous = ds.DefaultValue
			}
		}
	}
	require.NotEmpty(t, previous, name)
	set := func(ctx context.Context, v string) error {
		_, err := client.PostDynamicSetting(ctx, &protos.PostDynamicSettingRequest{Name: name, Value: proto.String(v)})
		return err
	}
	require.NoError(t, set(t.Context(), value))
	// t.Context() is already canceled when cleanups run
	t.Cleanup(func() { require.NoError(t, set(context.Background(), previous)) })
}
