# exp01_data_skew - before/after comparison

All values were captured from Spark's monitoring REST API at the end of each run
(the same data the Spark UI shows). Most recent run of each variant is used.

Single runs on one machine have noise of roughly +/-10-20%. Treat small differences as noise.

## Application-level metrics

| Metric | `before_sortmerge_no_aqe` | `optimized_aggregate_before_join` | `optimized_aqe_skew_join` | `optimized_salted_join` |
|---|---|---|---|---|
| Application ID | `app-20261124000409-0005` | `app-20261124000538-0008` | `app-20261124000440-0006` | `app-20261124000506-0007` |
| Wall-clock runtime of workload | 23.0 s | 16.9 s | 17.1 s | 23.9 s |
| Jobs | 3 | 3 | 6 | 5 |
| Stages run (skipped) | 6 (0) | 7 (0) | 6 (5) | 10 (0) |
| Tasks | 45 | 61 | 50 | 76 |
| Failed tasks | 0 | 0 | 0 | 0 |
| Input read | 458.1 MB | 458.1 MB | 458.1 MB | 642.2 MB |
| Shuffle write | 476.6 MB | 273.4 MB | 477.6 MB | 564.4 MB |
| Shuffle read | 476.6 MB | 273.4 MB | 481.6 MB | 564.4 MB |
| Spill (memory) | 704.0 MB | 0 B | 560.0 MB | 360.0 MB |
| Spill (disk) | 274.1 MB | 0 B | 210.8 MB | 123.8 MB |
| Total GC time (all tasks) | 1.5 s | 1.4 s | 1.5 s | 2.1 s |
| Max task duration (any stage) | 12.7 s | 4.2 s | 3.7 s | 5.1 s |
| Median task duration (all stages) | 487 ms | 467 ms | 460 ms | 418 ms |

## The heaviest shuffle stage in each run

The stage that read the most shuffle data (for a join query, the stage that performs the join).

| Metric | `before_sortmerge_no_aqe` | `optimized_aggregate_before_join` | `optimized_aqe_skew_join` | `optimized_salted_join` |
|---|---|---|---|---|
| Stage id | 4 | 5 | 6 | 8 |
| Tasks in stage | 16 | 16 | 21 | 16 |
| Stage duration | 12.7 s | 2.4 s | 6.6 s | 8.8 s |
| Median task duration | 518 ms | 518 ms | 466 ms | 2.0 s |
| Max task duration | 12.7 s | 1.1 s | 3.7 s | 5.1 s |
| Max / median duration | 24.5x | 2.1x | 8.0x | 2.5x |
| Median records per task | 624,292 | 597,134 | 626,962 | 1,934,248 |
| Max records per task | 21,626,652 | 601,920 | 4,782,631 | 3,580,589 |
| Median shuffle read per task | 10.9 MB | 8.8 MB | 10.9 MB | 32.4 MB |
| Max shuffle read per task | 309.8 MB | 8.8 MB | 68.4 MB | 57.8 MB |
| Disk spill in stage | 274.1 MB | 0 B | 210.8 MB | 123.8 MB |
| Max peak execution memory (one task) | 298.2 MB | 74.2 MB | 162.2 MB | 202.2 MB |

## Per-stage detail

### `before_sortmerge_no_aqe`  (`app-20261124000409-0005`)

| Stage | Status | Tasks | Stage time | Task median | Task max | Max/median duration | Max/median records | Shuffle read | Shuffle write | Disk spill |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | COMPLETE | 1 | 700 ms | 692 ms | 692 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 1 | COMPLETE | 1 | 54 ms | 53 ms | 53 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 2 | COMPLETE | 4 | 2.3 s | 1.9 s | 2.3 s | 1.2x | 1.0x | 0 B | 12.6 MB | 0 B |
| 3 | COMPLETE | 7 | 4.1 s | 2.4 s | 2.6 s | 1.1x | 1.0x | 0 B | 460.7 MB | 0 B |
| 4 | COMPLETE | 16 | 12.7 s | 518 ms | 12.7 s | 24.5x | 34.6x | 473.2 MB | 3.4 MB | 274.1 MB |
| 5 | COMPLETE | 16 | 988 ms | 172 ms | 487 ms | 2.8x | 1.1x | 3.4 MB | 0 B | 0 B |

### `optimized_aggregate_before_join`  (`app-20261124000538-0008`)

| Stage | Status | Tasks | Stage time | Task median | Task max | Max/median duration | Max/median records | Shuffle read | Shuffle write | Disk spill |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | COMPLETE | 1 | 709 ms | 704 ms | 704 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 1 | COMPLETE | 1 | 60 ms | 60 ms | 60 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 2 | COMPLETE | 7 | 6.1 s | 3.5 s | 4.2 s | 1.2x | 1.0x | 0 B | 130.9 MB | 0 B |
| 3 | COMPLETE | 4 | 2.7 s | 2.0 s | 2.7 s | 1.3x | 1.0x | 0 B | 12.6 MB | 0 B |
| 4 | COMPLETE | 16 | 2.5 s | 510 ms | 960 ms | 1.9x | 1.0x | 130.9 MB | 127.6 MB | 0 B |
| 5 | COMPLETE | 16 | 2.4 s | 518 ms | 1.1 s | 2.1x | 1.0x | 140.2 MB | 2.4 MB | 0 B |
| 6 | COMPLETE | 16 | 805 ms | 159 ms | 373 ms | 2.3x | 1.1x | 2.4 MB | 0 B | 0 B |

### `optimized_aqe_skew_join`  (`app-20261124000440-0006`)

| Stage | Status | Tasks | Stage time | Task median | Task max | Max/median duration | Max/median records | Shuffle read | Shuffle write | Disk spill |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | COMPLETE | 1 | 689 ms | 685 ms | 685 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 1 | COMPLETE | 1 | 611 ms | 611 ms | 611 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 2 | COMPLETE | 4 | 1.5 s | 1.5 s | 1.5 s | 1.0x | 1.0x | 0 B | 12.6 MB | 0 B |
| 3 | COMPLETE | 7 | 3.9 s | 2.5 s | 2.6 s | 1.1x | 1.0x | 0 B | 460.7 MB | 0 B |
| 4 | SKIPPED | | | | | | | | | |
| 5 | SKIPPED | | | | | | | | | |
| 6 | COMPLETE | 21 | 6.6 s | 466 ms | 3.7 s | 8.0x | 7.6x | 477.2 MB | 4.4 MB | 210.8 MB |
| 7 | SKIPPED | | | | | | | | | |
| 8 | SKIPPED | | | | | | | | | |
| 9 | SKIPPED | | | | | | | | | |
| 10 | COMPLETE | 16 | 876 ms | 168 ms | 426 ms | 2.5x | 1.1x | 4.4 MB | 0 B | 0 B |

### `optimized_salted_join`  (`app-20261124000506-0007`)

| Stage | Status | Tasks | Stage time | Task median | Task max | Max/median duration | Max/median records | Shuffle read | Shuffle write | Disk spill |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | COMPLETE | 1 | 662 ms | 658 ms | 658 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 1 | COMPLETE | 1 | 629 ms | 628 ms | 628 ms | 1.0x | - | 0 B | 0 B | 0 B |
| 2 | COMPLETE | 7 | 1.1 s | 937 ms | 982 ms | 1.0x | 1.0x | 0 B | 413 B | 0 B |
| 3 | COMPLETE | 1 | 122 ms | 121 ms | 121 ms | 1.0x | 1.0x | 413 B | 0 B | 0 B |
| 4 | COMPLETE | 7 | 3.2 s | 2.3 s | 2.4 s | 1.0x | 1.0x | 0 B | 41.7 MB | 0 B |
| 5 | COMPLETE | 16 | 929 ms | 167 ms | 512 ms | 3.1x | 1.0x | 41.7 MB | 0 B | 0 B |
| 6 | COMPLETE | 7 | 4.3 s | 2.3 s | 2.6 s | 1.1x | 1.0x | 0 B | 506.4 MB | 0 B |
| 7 | COMPLETE | 4 | 986 ms | 202 ms | 412 ms | 2.0x | 1.0x | 0 B | 12.9 MB | 0 B |
| 8 | COMPLETE | 16 | 8.8 s | 2.0 s | 5.1 s | 2.5x | 1.9x | 519.3 MB | 3.4 MB | 123.8 MB |
| 9 | COMPLETE | 16 | 851 ms | 162 ms | 382 ms | 2.4x | 1.1x | 3.4 MB | 0 B | 0 B |

## Join operators in the final physical plan

- `before_sortmerge_no_aqe`: SortMergeJoin
- `optimized_aggregate_before_join`: SortMergeJoin
- `optimized_aqe_skew_join`: SortMergeJoin(skew=true)
- `optimized_salted_join`: SortMergeJoin
