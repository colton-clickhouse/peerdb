package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

// ClickHouse can't store infinity: +-infinity date array elements become its DefaultTime (the Unix epoch) on both
// paths, so the snapshot row and the CDC row agree.
func (s ClickHouseSuite) Test_Array_Infinity_Elements_CH() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("infinity dates are a Postgres source case")
	}
	const srcTable = "test_array_infinity_ch"
	srcSchemaTable := s.attachSchemaSuffix(srcTable)
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, d date[])`, srcSchemaTable)))
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (1, ARRAY['2024-01-02','infinity','-infinity',NULL]::date[])`, srcSchemaTable)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTable),
		TableNameMapping: map[string]string{srcSchemaTable: srcTable},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
	EnvNoError(s.t, env, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s VALUES (2, ARRAY['2024-01-02','infinity','-infinity',NULL]::date[])`, srcSchemaTable)))

	var got []string
	EnvWaitFor(s.t, env, 3*time.Minute, "snapshot and cdc rows", func() bool {
		rows, err := s.GetRows(srcTable, "id, toString(d) AS d")
		if err != nil || len(rows.Records) != 2 {
			return false
		}
		got = got[:0]
		for _, r := range rows.Records {
			got = append(got, fmt.Sprint(r[1].Value()))
		}
		return true
	})
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
	for i, d := range got {
		require.Equal(s.t, "['2024-01-02','1970-01-01','1970-01-01','1970-01-01']", strings.ReplaceAll(d, " ", ""),
			"row %d: infinity and NULL elements are ClickHouse's DefaultTime", i+1)
	}
}
