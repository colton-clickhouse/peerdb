package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

// specialValueColumns are the columns of the special-values tests: NaN and ±Infinity numerics and floats, ±infinity
// dates and timestamps, scalar and in arrays, all of which Postgres stores.
const specialValueColumns = `id INT8 PRIMARY KEY,
	d DECIMAL, d_nn DECIMAL NOT NULL, d_b DECIMAL(10, 2), f8 FLOAT8, f4 FLOAT4,
	dt DATE, ts TIMESTAMP, tz TIMESTAMPTZ,
	ad DECIMAL[], af FLOAT8[], adt DATE[], ats TIMESTAMP[], atz TIMESTAMPTZ[]`

// specialValueRows has one row per sign: %[2]d is the id of the first of three. The bounded DECIMAL(10, 2) gets NaN
// only: Postgres can't store ±Infinity in numeric(p, s) (CockroachDB can).
const specialValueRows = `(%[2]d, 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'infinity', 'infinity', 'infinity',
		ARRAY['NaN', 'Infinity', '-Infinity', NULL, 1.5]::DECIMAL[], ARRAY['NaN', 'Infinity', '-Infinity', NULL, 1.5]::FLOAT8[],
		ARRAY['infinity', '-infinity', NULL, '2024-01-01']::DATE[],
		ARRAY['infinity', '-infinity', NULL, '2024-01-01 12:00:00']::TIMESTAMP[],
		ARRAY['infinity', '-infinity', NULL, '2024-01-01 12:00:00+00']::TIMESTAMPTZ[]),
	(%[2]d + 1, 'Infinity', 'Infinity', 1.5, 'Infinity', 'Infinity', '-infinity', '-infinity', '-infinity',
		NULL, NULL, NULL, NULL, NULL),
	(%[2]d + 2, '-Infinity', '-Infinity', NULL, '-Infinity', '-Infinity', '2024-01-01', '2024-01-01 12:00:00', '2024-01-01 12:00:00+00',
		ARRAY[]::DECIMAL[], ARRAY[]::FLOAT8[], ARRAY[]::DATE[], ARRAY[]::TIMESTAMP[], ARRAY[]::TIMESTAMPTZ[])`

// specialValueExpected is what a faithful copy of specialValueRows holds, as Postgres literals per row offset.
var specialValueExpected = []map[string]string{{
	"d": `'NaN'::numeric`, "d_nn": `'NaN'::numeric`, "d_b": `'NaN'::numeric`, "f8": `'NaN'::float8`, "f4": `'NaN'::float4`,
	"dt": `'infinity'::date`, "ts": `'infinity'::timestamp`, "tz": `'infinity'::timestamptz`,
	"ad":  `ARRAY['NaN', 'Infinity', '-Infinity', NULL, 1.5]::numeric[]`,
	"af":  `ARRAY['NaN', 'Infinity', '-Infinity', NULL, 1.5]::float8[]`,
	"adt": `ARRAY['infinity', '-infinity', NULL, '2024-01-01']::date[]`,
	"ats": `ARRAY['infinity', '-infinity', NULL, '2024-01-01 12:00:00']::timestamp[]`,
	"atz": `ARRAY['infinity', '-infinity', NULL, '2024-01-01 12:00:00+00']::timestamptz[]`,
}, {
	"d": `'Infinity'::numeric`, "d_nn": `'Infinity'::numeric`, "d_b": `1.5`, "f8": `'Infinity'::float8`, "f4": `'Infinity'::float4`,
	"dt": `'-infinity'::date`, "ts": `'-infinity'::timestamp`, "tz": `'-infinity'::timestamptz`,
	"ad": `NULL`, "af": `NULL`, "adt": `NULL`, "ats": `NULL`, "atz": `NULL`,
}, {
	"d": `'-Infinity'::numeric`, "d_nn": `'-Infinity'::numeric`, "d_b": `NULL`, "f8": `'-Infinity'::float8`, "f4": `'-Infinity'::float4`,
	"dt": `'2024-01-01'::date`, "ts": `'2024-01-01 12:00:00'::timestamp`, "tz": `'2024-01-01 12:00:00+00'::timestamptz`,
	"ad": `'{}'::numeric[]`, "af": `'{}'::float8[]`, "adt": `'{}'::date[]`, "ats": `'{}'::timestamp[]`, "atz": `'{}'::timestamptz[]`,
}}

// #6 and #10: NaN / ±Infinity decimals became NULL (0 when NOT NULL, 0 inside arrays) on both paths, infinity
// array elements 0001-01-01, CDC floats NaN / ±Infinity NULL, and in CDC an infinity date failed to parse while an
// infinity timestamp arrived as CockroachDB's sentinel (294277-01-01 / 4714-11-23 BC) and failed normalize. Postgres
// stores all of them. Rows 1-3 come from the snapshot, 11-13 from CDC inserts; an update moves row 1 to -Infinity.
func (s CRDBPGSuite) Test_Special_Values_Snapshot_And_CDC() {
	t := s.t
	srcTable := "special_values"
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (%s)`, srcFull, specialValueColumns)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+specialValueRows, srcFull, 1)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s`, dstFull)) == 3
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+specialValueRows, srcFull, 11)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %s SET d = '-Infinity', ad = ARRAY['-Infinity', 'NaN']::DECIMAL[], ts = '-infinity' WHERE id = 1`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows and the update", func() bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 6 AND bool_or(id = 1 AND d = '-Infinity'::numeric) FROM %s`, dstFull))
		return ok
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	var bad []string
	for _, base := range []int{1, 11} {
		for offset, expected := range specialValueExpected {
			id := base + offset
			for _, col := range []string{"d", "d_nn", "d_b", "f8", "f4", "dt", "ts", "tz", "ad", "af", "adt", "ats", "atz"} {
				want := expected[col]
				if id == 1 {
					// the update
					want = map[string]string{
						"d": `'-Infinity'::numeric`, "ad": `ARRAY['-Infinity', 'NaN']::numeric[]`, "ts": `'-infinity'::timestamp`,
					}[col]
					if want == "" {
						want = expected[col]
					}
				}
				where := fmt.Sprintf(`FROM %s WHERE id = %d`, dstFull, id)
				if ok, err := s.destBool(fmt.Sprintf(`SELECT %s IS NOT DISTINCT FROM %s %s`, col, want, where)); !ok {
					bad = append(bad, fmt.Sprintf("id %d %s: want %s, got %s (%v)",
						id, col, want, s.destText(fmt.Sprintf(`SELECT %s::text %s`, col, where)), err))
				}
			}
		}
	}
	if len(bad) > 0 {
		t.Errorf("%d special-value mismatches on the destination:\n  %s", len(bad), strings.Join(bad, "\n  "))
	}
}
