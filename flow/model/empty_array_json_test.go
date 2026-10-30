package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// A Postgres source's empty enum or text-form array ('{}') reaches every CDC destination through the record JSON
// (Snowflake, Kafka, Pub/Sub, Event Hubs and the Postgres raw table alike): it is [], and a NULL array is null. The
// parser returned a nil slice for '{}', which marshals as null, the same as a NULL array.
func TestEmptyPgArrayToJSON(t *testing.T) {
	items := NewRecordItems(3)
	items.AddColumn("empty_enum", types.QValueArrayEnum{Val: shared.ParsePgArrayToStringSlice([]byte("{}"), ',')})
	items.AddColumn("empty_text", types.QValueArrayString{Val: shared.ParsePgArrayToStringSlice([]byte("{}"), ',')})
	items.AddColumn("null_array", types.QValueNull(types.QValueKindArrayEnum))
	out, err := items.ToJSONWithOptions(NewToJSONOptions(nil, false))
	require.NoError(t, err)
	require.JSONEq(t, `{"empty_enum":[],"empty_text":[],"null_array":null}`, out)
}
