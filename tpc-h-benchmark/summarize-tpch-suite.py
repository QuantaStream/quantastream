#!/usr/bin/env python3
import re
import statistics
import sys
from pathlib import Path


RESULT_RE = re.compile(
    r"\b(?P<status>PASS|FAIL|XFAIL|XPASS|SKIP)\s+"
    r"(?P<id>\S+)"
    r"(?:\s+\[(?P<duration>[^\]]+)\])?"
)
RUN_RE = re.compile(r"===== run (?P<run>\d+)/(?P<runs>\d+) end status=(?P<status>\d+) elapsed=(?P<elapsed>\d+)s =====")


def parse_duration(text):
    if not text:
        return None
    text = text.strip()
    if text == "<1s":
        return 0
    if text.endswith("ms"):
        return float(text[:-2]) / 1000
    if text.endswith("s"):
        return float(text[:-1])
    if text.endswith("m"):
        return float(text[:-1]) * 60
    return None


def format_duration(seconds):
    if seconds is None:
        return "-"
    if seconds == 0:
        return "<1s"
    if seconds < 1:
        return f"{seconds:.3f}s"
    if seconds == int(seconds):
        return f"{int(seconds)}s"
    return f"{seconds:.1f}s"


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def summarize(log_path):
    timings = {}
    statuses = {}
    run_elapsed = []

    for line in log_path.read_text(errors="replace").splitlines():
        result = RESULT_RE.search(line)
        if result:
            query_id = result.group("id")
            status = result.group("status")
            statuses.setdefault(query_id, []).append(status)
            duration = parse_duration(result.group("duration"))
            if duration is not None:
                timings.setdefault(query_id, []).append(duration)
            continue

        run = RUN_RE.search(line)
        if run:
            run_elapsed.append(int(run.group("elapsed")))

    print(f"TPC-H suite summary: {log_path}")
    if run_elapsed:
        print(
            "runs: "
            f"{len(run_elapsed)}  "
            f"elapsed min/median/max: "
            f"{format_duration(min(run_elapsed))} / "
            f"{format_duration(statistics.median(run_elapsed))} / "
            f"{format_duration(max(run_elapsed))}"
        )
    print()

    rows = []
    for query_id, values in timings.items():
        rows.append(
            {
                "id": query_id,
                "runs": len(values),
                "min": min(values),
                "median": statistics.median(values),
                "p95": percentile(values, 0.95),
                "max": max(values),
                "statuses": ",".join(sorted(set(statuses.get(query_id, [])))),
            }
        )
    rows.sort(key=lambda row: (row["median"], row["max"], row["id"]), reverse=True)

    print(f"{'query':58} {'runs':>4} {'min':>8} {'median':>8} {'p95':>8} {'max':>8}  status")
    print(f"{'-' * 58} {'-' * 4:>4} {'-' * 8:>8} {'-' * 8:>8} {'-' * 8:>8} {'-' * 8:>8}  ------")
    for row in rows:
        print(
            f"{row['id'][:58]:58} "
            f"{row['runs']:>4} "
            f"{format_duration(row['min']):>8} "
            f"{format_duration(row['median']):>8} "
            f"{format_duration(row['p95']):>8} "
            f"{format_duration(row['max']):>8}  "
            f"{row['statuses']}"
        )


def main():
    if len(sys.argv) != 2:
        print("usage: summarize-tpch-suite.py <tpch-suite-log>", file=sys.stderr)
        return 2
    log_path = Path(sys.argv[1])
    if not log_path.is_file():
        print(f"log file not found: {log_path}", file=sys.stderr)
        return 2
    summarize(log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
