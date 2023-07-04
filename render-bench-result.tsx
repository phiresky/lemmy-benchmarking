// execute using deno deno run --allow-read render-bench-result.tsx runs/*pool-with-alive-check-async/info.json runs/*pool-with-alive-check/info.json runs/*pool-no-alive-check/info.json > ../lemmy-perf-data/comparisons/$(date -I)-pool-liveness-check.md
import React from "https://esm.sh/react@18.2.0";
import { renderToString } from "https://esm.sh/react-dom@18.2.0/server";
import { basename, dirname } from "https://deno.land/std@0.192.0/path/posix.ts";
type RowInfo = {
  query: "SELECT $1";
  calls: 2608407;
  total_exec_time: 5044.296157999897;
  mean_exec_time: 0.0019338608422689047;
  rows: 2608407;
  toplevel: boolean;
};
type Run = {
  dirname: string;
  total_time_s: 309.677769698;
  unqueue_time_s: 46.886420583;
  clear_time_s: 262.791349115;
  activityqueue_stats: "Some(Activity queue stats: pending: 0, running: 0, retries: 0, dead: 0, complete: 100000)";
  db_stats: {
    comment_count: 48716;
    post_count: 49807;
    comment_like_count: 4731;
    post_like_count: 30510;
    activity_count: 98649;
    statement_count: 1042;
    statement_call_count: 2735692;
    total_plan_time_s: 0.0;
    total_exec_time_s: 23129.88147680311;
    top_queries_by_call_count: RowInfo[];
    top_queries_by_mean_time: RowInfo[];
    top_queries_by_total_time: RowInfo[];
  };
  config: {
    local_server: "http://reddit.com.localhost:5313/";
    remote_server: "http://lemmy.localhost:8536/inbox";
    input_file: "crates/integration_testing/dump.jsonl.zst";
    federation_workers: 100;
    output_json: "runs/2023-07-03T17:14:28+00:00-pg-synchrounous-commit/info.json";
    runname: "pg-synchrounous-commit";
  };
};
function formatNumber(s: number, unit: string) {
  return (
    s.toLocaleString("en-US", {
      maximumSignificantDigits: 3,
      useGrouping: false,
    }) + ` ${unit}`
  );
}

function formatSeconds(s: number) {
  if (s < 0.1) return formatNumber(s * 1000, "ms");
  if (s > 120)
    return formatNumber((s / 60) | 0, "min") + " " + formatNumber(s % 60, "s");
  return formatNumber(s, "s");
}
function extractTable<T>(
  runs: T[],
  columns_: Record<string, (t: T) => JSX.Element | string>,
  transform: (r: {
    name: string;
    value: JSX.Element | string;
  }) => JSX.Element | string = (r) => r.value
) {
  const columns = Object.entries(columns_).map(([name, extractor]) => ({
    name,
    extractor,
  }));
  return (
    <table>
      {"\n"}
      <thead>
        <tr>
          {columns.map((col) => (
            <th>{col.name}</th>
          ))}
        </tr>
      </thead>
      {"\n"}
      <tbody>
        {runs.map((run) => (
          <tr>
            {columns.map((e) => (
              <td>{transform({ name: e.name, value: e.extractor(run) })}</td>
            ))}
            {"\n"}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
function formatDbTable(rows: RowInfo[], highlight: string) {
  let i = 0;
  return extractTable(
    rows,
    {
      Top: () => String(++i) + ".",
      Calls: (r) => String(r.calls),
      Rows: (r) => String(r.rows),
      Toplevel: (r) => (r.toplevel ? "Yes" : "No"),
      "Avg. Time": (r) => formatSeconds(r.mean_exec_time / 1000),
      "Total Time": (r) => formatSeconds(r.total_exec_time / 1000),
      Query: (r) => r.query,
    },
    (r) => (r.name === highlight ? <b>{r.value}</b> : r.value)
  );
}
function formatDbTables(
  runs: Run[],
  get: (r: Run) => RowInfo[],
  highlight: string
) {
  return (
    <div>
      {runs.map((run) => (
        <div>
          <h4>Run: {run.config.runname}</h4>
          {formatDbTable(get(run), highlight)}
        </div>
      ))}
    </div>
  );
}
async function load() {
  const runs = await Promise.all(
    Deno.args.map(async (fname) => ({
      ...(JSON.parse(await Deno.readTextFile(fname)) as Run),
      dirname: basename(dirname(fname)),
    }))
  );
  const bench = "Import-Reddit-Dump-v1";
  const benchDescription = (
    <p>
      The benchmark <code>Import-Reddit-Dump-v1</code> reads a real dump of
      reddit content from December 2022, limited to 100k events, and uploads it
      to a lemmy server as if reddit was another federated instance. The votes
      use the real counts but simulated users and times (uniformly randomly
      between 0 and 2h after the post/comment). The benchmarking code is
      currently in a shitty state in{" "}
      <a href="https://github.com/phiresky/lemmy/tree/reddit-importer">
        this branch.
      </a>
    </p>
  );

  function Summary(p: React.PropsWithChildren) {
    return (
      <summary>
        <h2>{p.children}</h2>
      </summary>
    );
  }

  const res = renderToString(
    <div>
      {extractTable(runs, {
        Benchmark: (r) => bench,
        Experiment: (run) => run.config.runname,
        "Activity Count": (run) => String(run.db_stats.activity_count),
        "Total Duration": (run) => formatSeconds(run.total_time_s),
        "Database Exec Time": (run) =>
          formatSeconds(run.db_stats.total_exec_time_s),
        "Database Query Count": (run) =>
          String(run.db_stats.statement_call_count),
      })}
      <details>
        <Summary>Show details</Summary>
        <h3>Benchmark explanation</h3>
        {benchDescription}
        <h3>Column Explanations</h3>
        <table>
          {[
            [
              "Activity Count",
              "After Running the import, this is the number of rows in the activity table. It should be almost equal to the number of events uploaded",
            ],
            [
              "Total Duration",
              "The total wall time it took for the upload to complete (including clearing the federation queue)",
            ],
            [
              "Database Exec Time",
              <>
                The total time the database spent executing queries. Higher than
                the wall time since the database does parallel queries. (select
                sum(total_exec_time) from pg_stat_statements) (excluding{" "}
                <a href="https://github.com/weiznich/diesel_async/discussions/89">
                  SELECT 1
                </a>
                )
              </>,
            ],
            [
              "Database Query Count",
              <>
                Total number of database queries executed (select sum(calls)
                from pg_stat_statements) (excluding{" "}
                <a href="https://github.com/weiznich/diesel_async/discussions/89">
                  SELECT 1
                </a>
                )
              </>,
            ],
          ].map((r) => (
            <tr>
              {r.map((v) => (
                <td>{v}</td>
              ))}
            </tr>
          ))}
        </table>
        <h3>Details</h3>
        {extractTable(runs, {
          Experiment: (run) => run.config.runname,
          Link: (run) => (
            <a
              href={`https://github.com/phiresky/lemmy-perf-data/tree/main/runs/${encodeURIComponent(
                run.dirname
              )}`}
            >
              {run.dirname}
            </a>
          ),
          "Activityqueue-Stats": (r) => r.activityqueue_stats,
          "Unique db statements": (r) => String(r.db_stats.statement_count),
          "Post Count": (run) => String(run.db_stats.post_count),
          "Comment Count": (run) => String(run.db_stats.comment_count),
          "Post Vote Count": (run) => String(run.db_stats.post_like_count),
          "Comment Vote Count": (run) =>
            String(run.db_stats.comment_like_count),
        })}
      </details>
      <details>
        <Summary>Top Queries by call count</Summary>
        {formatDbTables(
          runs,
          (r) => r.db_stats.top_queries_by_call_count.slice(0, 10),
          "Calls"
        )}
      </details>
      <details>
        <Summary>Top Queries by total time</Summary>
        {formatDbTables(
          runs,
          (r) => r.db_stats.top_queries_by_total_time.slice(0, 10),
          "Total Time"
        )}
      </details>
      <details>
        <Summary>Flamegraphs</Summary>
        (click for interactive)
        {runs.map((run) => {
          const url = `https://phiresky.github.io/lemmy-perf-data/runs/${run.dirname}/perf.data.flamegraph.svg`;
          return (
            <div>
              <a href={url}>
                <img src={url} />
              </a>
            </div>
          );
        })}
      </details>
    </div>
  );

  console.log(res.replace(/&quot;/g, '"'));
}

load();
