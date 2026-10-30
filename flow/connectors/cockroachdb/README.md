# CockroachDB source: known limitations

Values or changes this connector can't replicate exactly, and why. Each is a deliberate choice or a limit of
CockroachDB's changefeed format; revisit them if they turn out to matter in practice.

- **A top-level JSONB `null` becomes SQL NULL** on the target, on both the snapshot and CDC paths. Changefeed JSON
  writes a JSONB `null` and SQL NULL alike. The changefeed option `encode_json_value_null_as_object` would tell them
  apart, but it marks JSON null as `{"__crdb_json_null__": true}` without escaping a user object of exactly that value,
  so that object would turn into JSON null. Nested nulls (`[null]`, `{"a": null}`) are kept.
- **XYM geometries in a column without a declared dimension** (`GEOMETRY`, not `GEOMETRY(POINTM)`) arrive through CDC
  as XYZ: changefeed GeoJSON has no M dimension, so 3 coordinates are read as Z. The snapshot keeps M. Declaring the
  column's dimension avoids it. XYZM, and XYM in a column typed with M, are kept on both paths.
- **`ADD COLUMN … DEFAULT` during CDC** leaves the rows that existed before it NULL in the new target column. The
  changefeed runs with `schema_change_policy='nobackfill'`, and the target column is added without the default.
- **Column defaults**, including `REGIONAL BY ROW`'s hidden `crdb_region`, aren't created on the target.
- **Unconstrained geometry / geography SRIDs** aren't kept: changefeed GeoJSON has none, so the snapshot drops them too
  (geography is 4326 on the target). Constrained columns (`GEOMETRY(POINT,4326)`) keep their SRID.
- **A changefeed cursor that never advances** (resolved timestamps held back for good) shows as RUNNING with growing
  lag (the resolved-lag warning and the lag metrics), not STALLED.

## Session settings

The connector pins `timezone`, `DateStyle` and `intervalstyle` for its sessions, since the snapshot reads some values
as text. It doesn't pin `extra_float_digits`, and that's safe today: a role or database default of 0 or less shortens
floats' text (`FLOAT8::STRING`), but the snapshot reads floats in pgx's binary format and only casts intervals to text,
and changefeed JSON keeps floats' full precision whatever the session's setting. A new text read of a float would need
the setting pinned.
