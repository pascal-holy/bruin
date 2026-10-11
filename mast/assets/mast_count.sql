/* @bruin

name: mast_count
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace

depends:
  - mast

description: |
  # MaStR row count

  Single-row sanity check on the `mast` table: how many MaStR solar records were
  materialized by the last successful run.

  Depends on [mast](/pipelines/mast/assets/mast.py), so Bruin runs it only after
  that asset completes. It is cheap enough to sit in front of the real pipelines
  as a fast failure signal — if MaStR returns an empty or truncated payload,
  this asset surfaces it immediately rather than downstream models silently
  reporting zero.

  Expect roughly 1.5M rows against the current filter
  (`Anzahl der Solar-Module` < 5, `Betriebs-Status` 35, `Energieträger` 2495).
  The count fluctuates as the registry is corrected upstream, so treat a large
  drop as a signal to check [mast](/pipelines/mast/assets/mast.py), not as a bug
  in this query.

@bruin */

SELECT COUNT(*) AS row_count
FROM mast;