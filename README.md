# Query Engine Testings

If not using direnv, make sure to source `.envrc`.

## Benchmarks

We settled on five tests:

1. Join: left join `COMPUTER_NAME.eid` on `CVES.eid`.
1. Transform: multiply CVSS score by 10.
1. Filter: Remove all rows from CVE where CVSS < 5 (edit: actually .5, since data was generated from 0-1 instead of 0-10).
1. Aggregate/GroupBy: SUM of CVSS scores grouped by EID.

## Preliminary Results

Run on MBP -- need to normalize on common machinery. In the mean time please
ensure you have no `kernal_task` or anything else taking up resources, and run
tests at least twice and throw out outliers. (Times in seconds.)

Max Mem should be calculated using `/usr/bin/time -l`

### 100 M

| Query Engine                                   | Join     | Transform  | Filter     | Aggregate/Groupby | Max Mem           | Rough CPU        |
| -----------------------------------------------| -------  | ---------- | --------   | ----------------- | ----------------- | ---------------- |
| DataFusion  (batch=65536, num partitions = 12) | 3.93 s   | 182ms      | 618ms      | 19                | 6.7 GB            | 700%             |
| Julia/DataFrames (min/max)                     | 0.7/2.2 s| 1.7/1.8 s  | 0.46/1.47 s| 9.8/11 s          | 0.95 GB           | 100%             |
| Polars                                         | 22 s     | 0.4 s      | 2 s        | 9.8 s             | 1.8 GB            | 700%             |


Notes:

- Julia/Dataframes:
  - This code ran single-core.
  - Max mem reported is `maximum resident set size` as reported by `time -l`.
  - Each benchmark was run for 6 iterations; the mean / stddev was computed for the last 5 of these
    (attempt to discard the JAOT compilation).
  - The transform was in-place.
  - Filter was `x -> x > 0.5`.
  - Risk calc omitted the `log` factor.
