# Lemmy Benchmarking

Rust code that instruments lemmy server to check for performance.

Uses PostgreSQL pg_stat_statements to get query counts and [perf](https://perf.wiki.kernel.org/index.php/Main_Page) to build flame graphs.

Uses deno+react to render nice markdown/html files of the results. Results are uploaded to https://github.com/phiresky/lemmy-perf-data/tree/main/comparisons (currently manually).

## Implemented Benchmarks

### Import-Reddit-Dump

<p>
    The benchmark <code>Import-Reddit-Dump-v1</code> reads a real
    dump of reddit content from December 2022, limited to 100k events, and uploads it to a lemmy server
    as if reddit was another federated instance. The votes use the real counts
    but simulated users and times (uniformly randomly between 0 and 2h after
    the post/comment).
</p>

Todo:

- add warmup phase to improve comment/post vote/c, vote/p ratios.
- log memory usage while benchmarking (getrusage or wait3?)


### Todo: user-facing benchmark

Simulate user load (posting, commenting, upvoting)