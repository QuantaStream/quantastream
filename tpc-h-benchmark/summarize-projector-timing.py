#!/usr/bin/env python3
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


TIMING_RE = re.compile(
    r"PROJECTOR_TIMING table=(?P<table>\S+) "
    r"fields=(?P<fields>\S*) "
    r"found_set=(?P<found_set>\d+) "
    r"cacheable=(?P<cacheable>\S+) "
    r"cache_hits=(?P<cache_hits>\d+) "
    r"cache_misses=(?P<cache_misses>\d+)"
    r"(?: bsi_fields=(?P<bsi_fields>\d+) bitmap_fields=(?P<bitmap_fields>\d+))? "
    r"elapsed=(?P<elapsed>\S+)"
)


def parse_duration(text):
    if text == "0s":
        return 0.0
    units = [
        ("ns", 1 / 1_000_000_000),
        ("\u00b5s", 1 / 1_000_000),
        ("us", 1 / 1_000_000),
        ("ms", 1 / 1_000),
        ("s", 1),
        ("m", 60),
        ("h", 3600),
    ]
    for suffix, multiplier in units:
        if text.endswith(suffix):
            try:
                return float(text[: -len(suffix)]) * multiplier
            except ValueError:
                return None
    return None


def format_duration(seconds):
    if seconds is None:
        return "-"
    if seconds == 0:
        return "0s"
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.0f}us"
    if seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    if seconds == int(seconds):
        return f"{int(seconds)}s"
    return f"{seconds:.2f}s"


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
    groups = defaultdict(lambda: {
        "elapsed": [],
        "miss_elapsed": [],
        "found_sets": [],
        "cache_hits": 0,
        "cache_misses": 0,
        "miss_calls": 0,
        "hit_only_calls": 0,
        "bsi_fields": 0,
        "bitmap_fields": 0,
    })
    total_events = 0

    for line in log_path.read_text(errors="replace").splitlines():
        match = TIMING_RE.search(line)
        if not match:
            continue
        total_events += 1
        table = match.group("table")
        fields = match.group("fields") or "-"
        key = (table, fields)
        elapsed = parse_duration(match.group("elapsed"))
        if elapsed is not None:
            groups[key]["elapsed"].append(elapsed)
        cache_hits = int(match.group("cache_hits"))
        cache_misses = int(match.group("cache_misses"))
        if cache_misses > 0:
            groups[key]["miss_calls"] += 1
            if elapsed is not None:
                groups[key]["miss_elapsed"].append(elapsed)
        elif cache_hits > 0:
            groups[key]["hit_only_calls"] += 1
        groups[key]["found_sets"].append(int(match.group("found_set")))
        groups[key]["cache_hits"] += cache_hits
        groups[key]["cache_misses"] += cache_misses
        groups[key]["bsi_fields"] += int(match.group("bsi_fields") or 0)
        groups[key]["bitmap_fields"] += int(match.group("bitmap_fields") or 0)

    print(f"PROJECTOR_TIMING summary: {log_path}")
    print(f"events={total_events} groups={len(groups)}")
    print()
    print(
        f"{'table':14} {'fields':42} {'calls':>6} {'total':>9} {'median':>9} "
        f"{'p95':>9} {'max':>9} {'avg_fs':>10} {'hits':>8} {'misses':>8} {'bsi':>6} {'bitmap':>6}"
    )
    print(
        f"{'-' * 14:14} {'-' * 42:42} {'-' * 6:>6} {'-' * 9:>9} {'-' * 9:>9} "
        f"{'-' * 9:>9} {'-' * 9:>9} {'-' * 10:>10} {'-' * 8:>8} {'-' * 8:>8} {'-' * 6:>6} {'-' * 6:>6}"
    )

    rows = []
    for (table, fields), stats in groups.items():
        elapsed_values = stats["elapsed"]
        total = sum(elapsed_values)
        rows.append((total, table, fields, stats))

    rows.sort(reverse=True)
    for _, table, fields, stats in rows:
        elapsed_values = stats["elapsed"]
        found_sets = stats["found_sets"]
        print(
            f"{table[:14]:14} "
            f"{fields[:42]:42} "
            f"{len(elapsed_values):>6} "
            f"{format_duration(sum(elapsed_values)):>9} "
            f"{format_duration(statistics.median(elapsed_values) if elapsed_values else None):>9} "
            f"{format_duration(percentile(elapsed_values, 0.95)):>9} "
            f"{format_duration(max(elapsed_values) if elapsed_values else None):>9} "
            f"{int(sum(found_sets) / len(found_sets)) if found_sets else 0:>10} "
            f"{stats['cache_hits']:>8} "
            f"{stats['cache_misses']:>8} "
            f"{stats['bsi_fields']:>6} "
            f"{stats['bitmap_fields']:>6}"
        )

    miss_rows = []
    for (table, fields), stats in groups.items():
        miss_elapsed = stats["miss_elapsed"]
        miss_total = sum(miss_elapsed)
        if stats["miss_calls"] > 0 or miss_total > 0:
            miss_rows.append((miss_total, table, fields, stats))

    if miss_rows:
        print()
        print("miss-heavy groups:")
        print(
            f"{'table':14} {'fields':42} {'miss_calls':>10} {'hit_only':>8} "
            f"{'miss_total':>10} {'miss_median':>11} {'miss_p95':>9} {'misses':>8} {'hits':>8}"
        )
        print(
            f"{'-' * 14:14} {'-' * 42:42} {'-' * 10:>10} {'-' * 8:>8} "
            f"{'-' * 10:>10} {'-' * 11:>11} {'-' * 9:>9} {'-' * 8:>8} {'-' * 8:>8}"
        )
        miss_rows.sort(reverse=True)
        for _, table, fields, stats in miss_rows:
            miss_elapsed = stats["miss_elapsed"]
            print(
                f"{table[:14]:14} "
                f"{fields[:42]:42} "
                f"{stats['miss_calls']:>10} "
                f"{stats['hit_only_calls']:>8} "
                f"{format_duration(sum(miss_elapsed)):>10} "
                f"{format_duration(statistics.median(miss_elapsed) if miss_elapsed else None):>11} "
                f"{format_duration(percentile(miss_elapsed, 0.95)):>9} "
                f"{stats['cache_misses']:>8} "
                f"{stats['cache_hits']:>8}"
            )


def main():
    if len(sys.argv) != 2:
        print("usage: summarize-projector-timing.py <projector-timing-log>", file=sys.stderr)
        return 2
    log_path = Path(sys.argv[1])
    if not log_path.is_file():
        print(f"log file not found: {log_path}", file=sys.stderr)
        return 2
    summarize(log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
