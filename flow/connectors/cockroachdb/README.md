# CockroachDB source: known limitations

Values or changes this connector can't replicate exactly, and why. Each is a deliberate choice or a limit of
CockroachDB's changefeed format; revisit them if they turn out to matter in practice.

## Values

- **A top-level JSONB `null` becomes SQL NULL** on the target, on both the snapshot and CDC paths. Changefeed JSON
  writes a JSONB `null` and SQL NULL alike. The changefeed option `encode_json_value_null_as_object` would tell them
  apart, but it marks JSON null as `{"__crdb_json_null__": true}` without escaping a user object of exactly that value,
  so that object would turn into JSON null. Nested nulls (`[null]`, `{"a": null}`) are kept.
- **Geometries with an M dimension stop CDC**, with an `UNSUPPORTED_GEOMETRY` error: changefeed GeoJSON has no M
  dimension, so CDC can't tell what the coordinates are. That covers XYZM values (4 coordinates) and any value in a
  column whose type has M without Z (`GEOMETRY(POINTM)`). The snapshot reads EWKB and keeps them. To replicate such a
  column, drop M at the source (`ST_Force3DZ` or `ST_Force2D`) or exclude the column. XYM values in a column without a
  declared dimension (`GEOMETRY`) can't be told apart from XYZ, and arrive through CDC as XYZ.
- **Unconstrained geometry / geography SRIDs** aren't kept: changefeed GeoJSON has none, so the snapshot drops them too
  (geography is 4326 on the target). Constrained columns (`GEOMETRY(POINT,4326)`) keep their SRID.
- **`TIME '24:00:00'` becomes `23:59:59.999999`**, on both paths and every destination, as before. In a table keyed on
  a `TIME` column holding both values, the two rows merge into one on the target.
- **Infinite dates and timestamps on destinations other than Postgres** (ClickHouse) become NULL, or the destination's
  default time (1970-01-01 on ClickHouse) in a `NOT NULL` column or inside an array, on both paths. Postgres keeps
  them.
- **A `TIMESTAMP[]` element after year 9999** fails CDC on destinations other than Postgres: the raw rows' JSON has no
  form for it. Postgres destinations, and scalar timestamps everywhere, are fine.

## Postgres destinations

- **Numeric `Infinity` needs Postgres 14 or later.** An older destination rejects it, and the snapshot or CDC batch
  fails; nothing is rewritten. `NaN` works on every version.
- **NUL bytes (U+0000) in strings** fail the snapshot (COPY refuses them) or the CDC batch (the raw table's JSONB
  does), since Postgres text can't hold them. Set `PEERDB_PG_STRIP_NUL_BYTES=true` in the mirror's env to remove them
  instead; the written value then differs from the source.
- **`TIMETZ` into a pre-created column of another type**: a `time` column gets the UTC time of day and a `text` column
  the time as Postgres prints a `timetz` in UTC (`04:30:00+00`), the same on both paths. A `bytea` column fails the
  snapshot and CDC (`bytea` takes binary source columns only). A `timetz` column gets the time in UTC (`+00`): the
  source's offset isn't kept.
- **`OID` values above 2^31 - 1** don't fit the `integer` column PeerDB creates for an `OID`, and the snapshot or CDC
  batch fails. Set the column's `destination_type` to `bigint`.
- **`INTERVAL` and `INTERVAL[]`** become `text` / `text[]` holding PeerDB's interval JSON (`{"hours":4,...}`). Set the
  column's `destination_type` to `interval` / `interval[]` in the mirror's column settings to get intervals, on both
  paths; values whose months or days don't fit Postgres's 32 bits fail the batch.

## Schema changes

- **No DDL on a table while its initial snapshot is set up.** The mirror reads the schema at setup and the snapshot
  reads rows at a timestamp taken just after; a change in between fails the snapshot (resync the mirror). Changes
  after the snapshot timestamp are fine: the snapshot reads the columns as they were at it.
- **Changing a table's primary key during CDC** (`ALTER PRIMARY KEY`) needs a resync: the target stays keyed on the old
  columns. Only a key-only delete that lacks one of the target's key columns fails (`PRIMARY_KEY_CHANGED`); a reordered
  or widened key isn't detected.
- **`ADD COLUMN … DEFAULT` during CDC** leaves the rows that existed before it NULL in the new target column. The
  changefeed runs with `schema_change_policy='nobackfill'`, and the target column is added without the default.
- **Column defaults**, including `REGIONAL BY ROW`'s hidden `crdb_region`, aren't created on the target.
- **Row-level TTL's hidden column, `crdb_internal_expiration`,** replicates as a regular `NOT NULL` column without a
  default, so inserts that don't set it fail on the target after cutover. Exclude it in the table mapping to leave it
  out, on both paths, also when TTL is enabled mid-stream; a keyless table's hidden `rowid` stays.

## Mirror health

- **A changefeed cursor that never advances** (resolved timestamps held back for good) shows as RUNNING with growing
  lag (the resolved-lag warning and the lag metrics).

## Session settings

The connector pins `timezone`, `DateStyle` and `intervalstyle` for its sessions, since the snapshot reads some values
as text. It doesn't pin `extra_float_digits`, and that's safe today: a role or database default of 0 or less shortens
floats' text (`FLOAT8::STRING`), but the snapshot reads floats in pgx's binary format and only casts intervals to text,
and changefeed JSON keeps floats' full precision whatever the session's setting. A new text read of a float would need
the setting pinned.
