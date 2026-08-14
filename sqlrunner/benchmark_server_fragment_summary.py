#!/usr/bin/env python3
"""Summarize server bitmap fragment probes from a benchmark report or jq dump."""

from __future__ import annotations

import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


DETAIL_RE = re.compile(r"(node|index|field|normalization)=([^\s]+)")
FRAGMENT_METRIC_RE = re.compile(r"^fragment_[0-9]+_(.+)$")
DURATION_RE = re.compile(r"^([0-9.]+)(ns|\u00b5s|us|ms|s|m|h)$")
DURATION_UNITS = {
    "ns": 1e-9,
    "\u00b5s": 1e-6,
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}


def usage() -> int:
    print("usage: benchmark_server_fragment_summary.py REPORT.json|probe-dump.txt", file=sys.stderr)
    return 2


def walk(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def object_value(obj: dict[str, Any]) -> str:
    for key in ("value", "intValue", "duration", "Value", "IntValue", "Duration"):
        if key in obj and obj[key] is not None:
            return str(obj[key])
    return ""


def detail_fields(detail: str) -> dict[str, str]:
    return {match.group(1): match.group(2) for match in DETAIL_RE.finditer(detail)}


def probes_from_json(path: Path) -> list[tuple[str, str, str, dict[str, str]]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    probes: list[tuple[str, str, str, dict[str, str]]] = []
    for obj in walk(data):
        name = obj.get("name") or obj.get("Name")
        if not name:
            continue
        detail = str(obj.get("detail") or obj.get("Detail") or "")
        fields = detail_fields(detail)
        if "normalization" not in fields:
            continue
        probes.append((str(name), object_value(obj), detail, fields))
    return probes


def probes_from_text(path: Path) -> list[tuple[str, str, str, dict[str, str]]]:
    probes: list[tuple[str, str, str, dict[str, str]]] = []
    line_re = re.compile(r"^([^=]+)=(.*?) detail=(.*)$")
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = line_re.match(line)
        if not match:
            continue
        name, value, detail = match.groups()
        fields = detail_fields(detail)
        if "normalization" not in fields:
            continue
        probes.append((name, value, detail, fields))
    return probes


def load_probes(path: Path) -> list[tuple[str, str, str, dict[str, str]]]:
    try:
        return probes_from_json(path)
    except json.JSONDecodeError:
        return probes_from_text(path)


def duration_seconds(value: str) -> float | None:
    if value == "0s":
        return 0.0
    match = DURATION_RE.match(value)
    if not match:
        return None
    return float(match.group(1)) * DURATION_UNITS[match.group(2)]


def fragment_metric(name: str) -> str:
    match = FRAGMENT_METRIC_RE.match(name)
    if not match:
        return name
    return match.group(1)


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    return statistics.median(values)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * pct)))
    return ordered[index]


def print_elapsed_summary(probes: list[tuple[str, str, str, dict[str, str]]]) -> None:
    elapsed: dict[tuple[str, str], list[float]] = defaultdict(list)
    rows: dict[tuple[str, str], list[int]] = defaultdict(list)
    value_counts: dict[tuple[str, str], list[int]] = defaultdict(list)
    for name, value, _, fields in probes:
        metric = fragment_metric(name)
        key = (fields.get("normalization", "?"), fields.get("field", "?"))
        if metric == "elapsed":
            seconds = duration_seconds(value)
            if seconds is not None:
                elapsed[key].append(seconds)
        elif metric == "rows":
            try:
                rows[key].append(int(value))
            except ValueError:
                pass
        elif metric == "value_count":
            try:
                value_counts[key].append(int(value))
            except ValueError:
                pass
    print("elapsed_by_branch_field")
    for key in sorted(elapsed):
        values = sorted(elapsed[key])
        branch, field = key
        row_values = rows.get(key, [])
        value_count_values = value_counts.get(key, [])
        row_part = f" rows_median={int(median(row_values))}" if row_values else ""
        value_count_part = f" values_median={int(median(value_count_values))}" if value_count_values else ""
        print(
            f"  {branch:<10} {field:<16} "
            f"median={median(values):.3f}s p95={percentile(values, 0.95):.3f}s max={max(values):.3f}s n={len(values)}"
            f"{row_part}{value_count_part}"
        )


def print_order_summary(probes: list[tuple[str, str, str, dict[str, str]]]) -> None:
    orders = Counter()
    multi_fragment = 0
    for name, value, _, fields in probes:
        if name != "fragment_order":
            continue
        orders[(fields.get("normalization", "?"), value)] += 1
        if " -> " in value:
            multi_fragment += 1
    print("fragment_orders")
    for (branch, value), count in sorted(orders.items()):
        print(f"  {branch:<10} x{count:<3} {value}")
    print(f"multi_fragment_orders={multi_fragment}")


def print_batch_eq_found_sets(probes: list[tuple[str, str, str, dict[str, str]]]) -> None:
    batch_keys: set[tuple[str, str]] = set()
    flags: dict[tuple[str, str], dict[str, Counter[str]]] = defaultdict(lambda: defaultdict(Counter))
    for name, value, _, fields in probes:
        metric = fragment_metric(name)
        key = (fields.get("normalization", "?"), fields.get("node", "?"))
        field = fields.get("field")
        if field == "l_partkey" and metric == "order" and "BATCH_EQ" in value:
            batch_keys.add(key)
        if field == "l_partkey" and metric in ("found_set_available", "found_set_used"):
            flags[key][metric][value] += 1
    print("batch_eq_l_partkey_found_sets")
    for branch, node in sorted(batch_keys):
        available = ",".join(f"{k}:{v}" for k, v in sorted(flags[(branch, node)]["found_set_available"].items()))
        used = ",".join(f"{k}:{v}" for k, v in sorted(flags[(branch, node)]["found_set_used"].items()))
        print(f"  {branch:<10} {node:<12} available={available or '-'} used={used or '-'}")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        return usage()
    path = Path(argv[1])
    if not path.exists():
        print(f"not found: {path}", file=sys.stderr)
        return 1
    probes = load_probes(path)
    print(f"server_fragment_probe_count={len(probes)}")
    print_elapsed_summary(probes)
    print_order_summary(probes)
    print_batch_eq_found_sets(probes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
