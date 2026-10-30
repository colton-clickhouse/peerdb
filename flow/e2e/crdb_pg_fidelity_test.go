package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/require"
)

// Value fidelity for types the connector maps lossily or decodes differently in snapshot (pgx binary)
// and CDC (changefeed JSON). Row 1 arrives by snapshot and row 2 (identical values) by CDC; row 4 by snapshot
// and then a CDC update of one column, which must leave the others as they were; row 3 by CDC with every column
// NULL. Every check runs against rows 1, 2 and 4 and all mismatches are reported together. The values are as the fixes
// in this stack decided them: an unconstrained GEOMETRY loses its SRID on both paths, since changefeed GeoJSON has
// none, and a top-level JSONB null is SQL NULL, since changefeed JSON writes it and SQL NULL alike. TIMETZ, NUL bytes,
// OIDs past 2^31 and BC dates have their own tests.
func (s CRDBPGSuite) Test_Type_Fidelity_Snapshot_And_CDC() {
	t := s.t
	srcTable := "type_fidelity"
	schema := Schema(s)
	srcFull := AttachSchema(s, srcTable)
	dstFull := s.DestinationTable(srcTable)

	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %s.mood AS ENUM ('happy', 'sad')`, schema)))
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id INT8 PRIMARY KEY,
		c_dec_nan DECIMAL, c_dec_inf DECIMAL, c_dec_big DECIMAL,
		c_interval INTERVAL, c_time24 TIME,
		c_jsonb JSONB, c_jsonb_null JSONB,
		c_int_arr INT8[], c_text_arr STRING[],
		c_geom GEOMETRY, c_bytes_empty BYTES, c_str_empty STRING,
		c_mood %s.mood, c_bit BIT(4), c_inet INET, c_uuid UUID
	)`, srcFull, schema)))
	values := `(%d,
		'NaN', 'Infinity', 12345678901234567890.123456789012345678,
		'1 year 2 mons 3 days 04:05:06.789', '24:00:00',
		'{"b": 1, "a": [1, null]}', 'null',
		ARRAY[1, NULL, 3], ARRAY['x', NULL, ''],
		'SRID=3857;POINT(1 2)', ''::BYTES, '',
		'sad', B'1010', '192.168.1.0/24', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')`
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+values+`, `+values, srcFull, 1, 4)))

	flowConnConfig := s.startMirror(srcTable)
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvWaitFor(t, env, 3*time.Minute, "snapshot rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (1, 4)`, dstFull)) == 2
	})
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES `+values, srcFull, 2)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s (id) VALUES (3)`, srcFull)))
	EnvNoError(t, env, s.source.Exec(t.Context(), fmt.Sprintf(`UPDATE %s SET c_str_empty = 'x' WHERE id = 4`, srcFull)))
	EnvWaitFor(t, env, 3*time.Minute, "cdc rows", func() bool {
		return s.destInt(fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (2, 3) OR (id = 4 AND c_str_empty = 'x')`,
			dstFull)) == 3
	})
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	// column -> (predicate that a faithful copy satisfies, expression to show when it doesn't)
	checks := []struct{ col, ok, show string }{
		{"c_dec_nan", `c_dec_nan::text = 'NaN'`, `c_dec_nan::text`},
		{"c_dec_inf", `c_dec_inf::text = 'Infinity'`, `c_dec_inf::text`},
		{"c_dec_big", `c_dec_big::text = '12345678901234567890.123456789012345678'`, `c_dec_big::text`},
		{"c_interval", `c_interval::text::interval::text = '1 year 2 mons 3 days 04:05:06.789'`, `c_interval::text`},
		{"c_time24", `c_time24::text = '24:00:00'`, `c_time24::text`},
		{"c_jsonb", `c_jsonb::jsonb::text = '{"a": [1, null], "b": 1}'`, `c_jsonb::text`},
		// a top-level JSONB null is SQL NULL on both paths, deliberately: changefeed JSON writes both as null (known
		// limitation, to revisit if it's a problem in practice)
		{"c_jsonb_null", `c_jsonb_null IS NULL`, `c_jsonb_null::text`},
		{"c_int_arr", `c_int_arr::text = '{1,NULL,3}'`, `c_int_arr::text`},
		{"c_text_arr", `c_text_arr::text = '{x,NULL,""}'`, `c_text_arr::text`},
		{"c_geom", `ST_SRID(c_geom) = 0 AND ST_AsText(c_geom) = 'POINT(1 2)'`, `ST_AsEWKT(c_geom)`},
		{"c_bytes_empty", `c_bytes_empty IS NOT NULL AND length(c_bytes_empty) = 0`, `coalesce(encode(c_bytes_empty, 'hex'), 'NULL')`},
		{"c_str_empty", `c_str_empty IS NOT NULL AND c_str_empty = ''`, `coalesce(c_str_empty, 'NULL')`},
		{"c_mood", `c_mood::text = 'sad'`, `c_mood::text`},
		{"c_bit", `c_bit::text = '1010'`, `c_bit::text`},
		{"c_inet", `c_inet::text = '192.168.1.0/24'`, `c_inet::text`},
		{"c_uuid", `c_uuid::text = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`, `c_uuid::text`},
	}
	types := map[string]string{}
	rows, err := s.dest.Conn().Query(t.Context(), `SELECT column_name, format_type(atttypid, atttypmod)
		FROM information_schema.columns c JOIN pg_attribute a ON a.attname = c.column_name
		AND a.attrelid = (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass
		WHERE table_schema = $1 AND table_name = $2`, "e2e_test_"+s.suffix, srcTable)
	require.NoError(t, err)
	for rows.Next() {
		var c, ty string
		require.NoError(t, rows.Scan(&c, &ty))
		types[c] = ty
	}
	require.NoError(t, rows.Err())

	var bad []string
	for _, c := range checks {
		ok, err := s.destBool(fmt.Sprintf(`SELECT %s IS NULL FROM %s WHERE id = 3`, c.col, dstFull))
		if err != nil || !ok {
			bad = append(bad, fmt.Sprintf("%-14s %-8s dest type %-28s not NULL (%v)", c.col, "cdc null", types[c.col], err))
		}
		for id, via := range map[int]string{1: "snapshot", 2: "cdc", 4: "update"} {
			if id == 4 && c.col == "c_str_empty" { // the column the update changed
				continue
			}
			ok, err := s.destBool(fmt.Sprintf(`SELECT %s FROM %s WHERE id = %d`, c.ok, dstFull, id))
			if !ok {
				got := s.destText(fmt.Sprintf(`SELECT %s FROM %s WHERE id = %d`, c.show, dstFull, id))
				if err != nil {
					got += " (check failed: " + err.Error() + ")"
				}
				bad = append(bad, fmt.Sprintf("%-14s %-8s dest type %-28s got %s", c.col, via, types[c.col], got))
			}
		}
	}
	if len(bad) > 0 {
		t.Errorf("%d value mismatches on the destination:\n  %s", len(bad), strings.Join(bad, "\n  "))
	}
}

// Which types stop the initial snapshot? One single-column table per type, all in one mirror; a table
// whose row never reaches the destination is retried forever and keeps the mirror in STATUS_SNAPSHOT.
// The failure message lists the blocking types (diagnostic for Test_Type_Fidelity_Snapshot_And_CDC).
func (s CRDBPGSuite) Test_Types_That_Block_Snapshot() {
	t := s.t
	schema := Schema(s)
	require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TYPE %s.mood2 AS ENUM ('happy', 'sad')`, schema)))
	cases := []struct{ name, typ, val string }{
		{"dec_nan", "DECIMAL", "'NaN'"}, {"dec_inf", "DECIMAL", "'Infinity'"},
		{"dec_big", "DECIMAL", "12345678901234567890.123456789012345678"},
		{"interval", "INTERVAL", "'1 year 2 mons 3 days 04:05:06.789'"}, {"timetz", "TIMETZ", "'13:14:15+05:30'"},
		{"time24", "TIME", "'24:00:00'"}, {"jsonb", "JSONB", `'{"b": 1, "a": [1, null]}'`}, {"jsonb_null", "JSONB", "'null'"},
		{"int_arr", "INT8[]", "ARRAY[1, NULL, 3]"}, {"text_arr", "STRING[]", "ARRAY['x', NULL, '']"},
		{"date_bc", "DATE", "'0044-03-15 BC'"}, {"ts_bc", "TIMESTAMP", "'0044-03-15 12:00:00 BC'"},
		{"date_inf", "DATE", "'infinity'"}, {"ts_inf", "TIMESTAMPTZ", "'infinity'"},
		{"geom", "GEOMETRY", "'SRID=3857;POINT(1 2)'"}, {"bytes_empty", "BYTES", "''::BYTES"},
		{"mood", schema + ".mood2", "'sad'"}, {"bit", "BIT(4)", "B'1010'"}, {"inet", "INET", "'192.168.1.0/24'"},
		{"interval_arr", "INTERVAL[]", "ARRAY['1 day'::INTERVAL]"}, {"str_nul", "STRING", `e'a\x00b'`},
	}
	tables := make([]string, 0, 2*len(cases))
	for _, c := range cases {
		tbl := "blk_" + c.name
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, v %s)`, AttachSchema(s, tbl), c.typ)))
		require.NoError(t, s.source.Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES (1, %s)`, AttachSchema(s, tbl), c.val)))
		tables = append(tables, tbl, tbl)
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, "blk"),
		TableMappings: TableMappings(s, tables...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 8
	flowConnConfig.SnapshotNumTablesInParallel = 8
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	arrived := func(tbl string) bool {
		ok, _ := s.destBool(fmt.Sprintf(`SELECT count(*) = 1 FROM %s`, s.DestinationTable(tbl)))
		return ok
	}
	// a slow snapshot would report types as blocking: give every table well past what a working one needs
	deadline := time.Now().Add(5 * time.Minute)
	var blocked []string
	for time.Now().Before(deadline) {
		blocked = blocked[:0]
		for _, c := range cases {
			if !arrived("blk_" + c.name) {
				blocked = append(blocked, c.name+" ("+c.typ+")")
			}
		}
		if len(blocked) == 0 {
			break
		}
		time.Sleep(5 * time.Second)
	}
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
	require.Empty(t, blocked, "types whose snapshot never reached the destination")
}
