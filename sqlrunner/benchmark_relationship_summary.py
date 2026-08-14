#!/usr/bin/env python3
"""Summarize relationship-join profile timings from a benchmark report."""

from __future__ import annotations

import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


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

EDGE_NAME_RE = re.compile(r"^graph_iter_(\d+)_edge_(\d+)_(.+)$")
EDGE_DESC_RE = re.compile(
    r"iter=(?P<iter>\d+)\s+edge=(?P<edge>\d+)\s+input=(?P<input>\d+)\s+"
    r"(?P<parent_role>[^:\s]+):(?P<parent_table>[^\[]+)\[(?P<parent_rows>\d+)\]\s+->\s+"
    r"(?P<child_role>[^:\s]+):(?P<child_table>[^\[]+)\[(?P<child_rows>\d+)\]"
)
KEY_VALUE_RE = re.compile(r"([A-Za-z0-9_]+)=([^\s]+)")

PHASE_NAMES = [
    "phase_execute_relationship_vector_elapsed",
    "q3_attribution_known_elapsed",
    "q3_attribution_graph_reduction_elapsed",
    "q3_attribution_preagg_total_elapsed",
    "q3_attribution_preagg_materialization_elapsed",
    "q3_attribution_preagg_accumulate_elapsed",
    "q3_attribution_preagg_storage_elapsed",
    "q3_attribution_preagg_storage_projection_elapsed",
    "q3_attribution_preagg_storage_aggregate_elapsed",
    "q3_attribution_final_stage_elapsed",
    "q3_attribution_final_materialization_elapsed",
    "q3_attribution_final_fetch_elapsed",
    "q3_attribution_final_attach_elapsed",
    "q3_attribution_sort_output_elapsed",
]

REDUCTION_TOTAL_NAMES = [
    "phase_graph_reduction_elapsed",
    "phase_graph_reduction_edge_reduce_total_elapsed",
    "phase_graph_reduction_edge_projection_total_elapsed",
    "phase_graph_reduction_parent_key_total_elapsed",
    "phase_graph_reduction_reverse_artifact_total_elapsed",
    "phase_graph_reduction_reverse_artifact_rpc_total_elapsed",
    "phase_graph_reduction_reverse_artifact_rpc_max_sum_elapsed",
    "phase_graph_reduction_value_vector_total_elapsed",
    "phase_graph_reduction_batch_equal_total_elapsed",
    "phase_graph_reduction_intersect_total_elapsed",
    "phase_graph_reduction_pair_total_elapsed",
    "phase_graph_reduction_child_retain_total_elapsed",
]

PREAGG_COUNTER_NAMES = [
    "graph_grouped_aggregate_preagg_rows",
    "graph_grouped_aggregate_preagg_groups",
    "graph_grouped_aggregate_preagg_storage_nodes",
    "graph_grouped_aggregate_preagg_storage_projection_shards_visited",
    "graph_grouped_aggregate_preagg_storage_projection_shards_retained",
    "graph_grouped_aggregate_preagg_storage_projection_rows_retained",
    "q3_attribution_input_line_rows",
    "q3_attribution_input_order_rows",
    "q3_attribution_final_materialization_rows",
    "q3_attribution_output_rows",
]

POLICY_EVENT_NAMES = [
    "graph_iter_1_edge_policy_applied_order",
    "graph_iter_1_edge_policy_input_order",
    "graph_iter_1_edge_policy_recommended_order",
    "graph_iter_1_edge_policy_apply_requested",
    "graph_iter_1_edge_policy_apply_eligible",
    "graph_iter_1_edge_policy_apply_reason",
    "graph_iter_1_single_pass_mode",
    "graph_iter_1_single_pass_applied",
    "graph_single_pass_applied",
    "graph_single_pass_reason",
    "graph_sink",
    "graph_reduced_roles",
]


def usage() -> int:
    print("usage: benchmark_relationship_summary.py REPORT.json [CASE_ID_SUBSTRING]", file=sys.stderr)
    return 2


def duration_seconds(value: str) -> float | None:
    if value == "0s":
        return 0.0
    match = DURATION_RE.match(value)
    if not match:
        return None
    return float(match.group(1)) * DURATION_UNITS[match.group(2)]


def int_value(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def object_value(obj: dict[str, Any]) -> str:
    for key in ("value", "intValue", "duration", "Value", "IntValue", "Duration"):
        if key in obj and obj[key] is not None:
            return str(obj[key])
    return ""


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * pct)))
    return ordered[index]


def fmt_seconds(seconds: float) -> str:
    if seconds >= 1:
        return f"{seconds:.3f}s"
    if seconds >= 0.001:
        return f"{seconds * 1000:.1f}ms"
    if seconds > 0:
        return f"{seconds * 1_000_000:.1f}us"
    return "0s"


def fmt_int(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:,}"


def walk(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def selected_cases(report: dict[str, Any], selector: str | None) -> list[dict[str, Any]]:
    cases = [case for case in report.get("cases", []) if isinstance(case, dict)]
    if selector:
        cases = [case for case in cases if selector in str(case.get("id", ""))]
    return cases


def profile_runs(case: dict[str, Any]) -> list[list[dict[str, Any]]]:
    runs = []
    for run in case.get("profile_runs", []):
        if isinstance(run, dict) and isinstance(run.get("profile"), list):
            runs.append([obj for obj in run["profile"] if isinstance(obj, dict)])
    if runs:
        return runs
    profile = case.get("profile")
    if isinstance(profile, list):
        return [[obj for obj in profile if isinstance(obj, dict)]]
    return []


def rows_by_name(run: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for obj in run:
        name = obj.get("name") or obj.get("Name")
        if name:
            out[str(name)].append(obj)
    return out


def values_for_name(runs: list[list[dict[str, Any]]], name: str) -> list[str]:
    values: list[str] = []
    for run in runs:
        for obj in rows_by_name(run).get(name, []):
            values.append(object_value(obj))
    return values


def timing_values(runs: list[list[dict[str, Any]]], name: str) -> list[float]:
    values = []
    for value in values_for_name(runs, name):
        seconds = duration_seconds(value)
        if seconds is not None:
            values.append(seconds)
    return values


def counter_values(runs: list[list[dict[str, Any]]], name: str) -> list[int]:
    values = []
    for value in values_for_name(runs, name):
        parsed = int_value(value)
        if parsed is not None:
            values.append(parsed)
    return values


def event_values(runs: list[list[dict[str, Any]]], name: str) -> list[str]:
    return values_for_name(runs, name)


def print_timing_table(title: str, runs: list[list[dict[str, Any]]], names: list[str]) -> None:
    print(title)
    for name in names:
        values = timing_values(runs, name)
        if not values:
            continue
        print(
            f"  {name}: median={fmt_seconds(median(values))} "
            f"p95={fmt_seconds(percentile(values, 0.95))} max={fmt_seconds(max(values))} n={len(values)}"
        )


def print_counter_table(title: str, runs: list[list[dict[str, Any]]], names: list[str]) -> None:
    print(title)
    for name in names:
        values = counter_values(runs, name)
        if not values:
            continue
        print(f"  {name}: median={fmt_int(int(median(values)))} max={fmt_int(max(values))} n={len(values)}")


def edge_entries(runs: list[list[dict[str, Any]]]) -> dict[tuple[int, int], dict[str, list[Any]]]:
    edges: dict[tuple[int, int], dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
    for run in runs:
        for obj in run:
            name = str(obj.get("name") or "")
            value = object_value(obj)
            match = EDGE_NAME_RE.match(name)
            if match:
                iter_id = int(match.group(1))
                edge_id = int(match.group(2))
                metric = match.group(3)
                key = (iter_id, edge_id)
                seconds = duration_seconds(value)
                parsed_int = int_value(value)
                if seconds is not None:
                    edges[key][metric].append(seconds)
                elif parsed_int is not None:
                    edges[key][metric].append(parsed_int)
                elif value != "":
                    edges[key][metric].append(value)
                continue
            if name.startswith("graph_reduction_edge_summary_"):
                parsed = parse_edge_summary(value)
                if parsed:
                    key = (int(parsed["iter"]), int(parsed["edge"]))
                    edges[key]["summary"].append(value)
                    edges[key]["edge_desc"].append(edge_description(parsed))
                    for metric, raw_value in parsed.items():
                        if metric in {"iter", "edge", "input", "parent_role", "parent_table", "child_role", "child_table"}:
                            continue
                        seconds = duration_seconds(raw_value)
                        parsed_int = int_value(raw_value)
                        if seconds is not None:
                            edges[key][f"summary_{metric}"].append(seconds)
                        elif parsed_int is not None:
                            edges[key][f"summary_{metric}"].append(parsed_int)
    return edges


def parse_edge_summary(value: str) -> dict[str, str] | None:
    match = EDGE_DESC_RE.search(value)
    if not match:
        return None
    parsed = match.groupdict()
    for key, raw_value in KEY_VALUE_RE.findall(value):
        parsed[key] = raw_value
    return parsed


def edge_description(parsed: dict[str, str]) -> str:
    return (
        f"iter={parsed['iter']} edge={parsed['edge']} input={parsed['input']} "
        f"{parsed['parent_role']}:{parsed['parent_table']}[{fmt_int(int(parsed['parent_rows']))}] -> "
        f"{parsed['child_role']}:{parsed['child_table']}[{fmt_int(int(parsed['child_rows']))}]"
    )


def scalar_median(values: list[Any]) -> Any:
    numeric = [value for value in values if isinstance(value, (int, float))]
    if len(numeric) == len(values) and numeric:
        return median(numeric)
    return Counter(str(value) for value in values).most_common(1)[0][0] if values else None


def print_edge_summary(runs: list[list[dict[str, Any]]]) -> None:
    edges = edge_entries(runs)
    print("edges_by_reduce")
    sortable = []
    for key, metrics in edges.items():
        reduce_values = metrics.get("reduce_elapsed") or metrics.get("summary_reduce") or []
        reduce_median = median([v for v in reduce_values if isinstance(v, float)])
        sortable.append((reduce_median, key, metrics))
    for reduce_median, key, metrics in sorted(sortable, reverse=True):
        desc = scalar_median(metrics.get("edge_desc", [])) or f"iter={key[0]} edge={key[1]}"
        parts = [f"reduce={fmt_seconds(reduce_median)}"]
        for metric in (
            "reverse_artifact_elapsed",
            "reverse_artifact_client_rpc_max_elapsed",
            "reverse_artifact_response_merge_elapsed",
            "reverse_artifact_narrow_elapsed",
            "parent_key_elapsed",
            "child_retain_elapsed",
            "value_vector_elapsed",
        ):
            values = [v for v in metrics.get(metric, []) if isinstance(v, float)]
            if values:
                parts.append(f"{metric.removesuffix('_elapsed')}={fmt_seconds(median(values))}")
        for metric in (
            "parent_rows",
            "child_rows",
            "joined_rows",
            "reverse_artifact_candidate_rows",
            "reverse_artifact_narrowed_rows",
            "matched_rows",
            "child_retain_rows",
        ):
            values = [v for v in metrics.get(metric, []) if isinstance(v, int)]
            if values:
                parts.append(f"{metric}={fmt_int(int(median(values)))}")
        mode = scalar_median(metrics.get("value_vector_mode", []))
        if mode:
            parts.append(f"value_vector_mode={mode}")
        ra_mode = scalar_median(metrics.get("reverse_artifact_mode", []))
        if ra_mode:
            parts.append(f"reverse_artifact_mode={ra_mode}")
        print(f"  {' '.join(parts)}  {desc}")


def print_policy_summary(runs: list[list[dict[str, Any]]]) -> None:
    print("policy")
    for name in POLICY_EVENT_NAMES:
        values = event_values(runs, name)
        if not values:
            continue
        counts = Counter(values)
        rendered = ", ".join(f"{value or '<empty>'}:{count}" for value, count in counts.most_common())
        print(f"  {name}: {rendered}")


def print_case(report: dict[str, Any], case: dict[str, Any]) -> None:
    runs = profile_runs(case)
    case_id = str(case.get("id", "<unknown>"))
    print(f"case={case_id}")
    print(
        f"status={case.get('status', '?')} runs={case.get('runs', len(runs))} "
        f"median={case.get('median_ms', '?')}ms min={case.get('min_ms', '?')}ms max={case.get('max_ms', '?')}ms"
    )
    metadata = report.get("metadata", {})
    if isinstance(metadata, dict):
        execution_path = metadata.get("execution_path")
        repo_commit = metadata.get("repo_commit")
        if execution_path or repo_commit:
            print(f"metadata execution_path={execution_path or '?'} repo_commit={repo_commit or '?'}")
    print_timing_table("q3_phase_timings", runs, PHASE_NAMES)
    print_timing_table("graph_reduction_totals", runs, REDUCTION_TOTAL_NAMES)
    print_counter_table("q3_rows", runs, PREAGG_COUNTER_NAMES)
    print_edge_summary(runs)
    print_policy_summary(runs)


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3):
        return usage()
    path = Path(argv[1])
    selector = argv[2] if len(argv) == 3 else None
    if not path.exists():
        print(f"not found: {path}", file=sys.stderr)
        return 1
    report = json.loads(path.read_text(encoding="utf-8"))
    cases = selected_cases(report, selector)
    if not cases:
        print("no matching cases", file=sys.stderr)
        return 1
    for index, case in enumerate(cases):
        if index:
            print()
        print_case(report, case)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
