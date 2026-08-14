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


DURATION_RE = re.compile(r"([0-9.]+)(ns|\u00b5s|us|ms|s|m|h)")
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
ALIGNMENT_EDGE_NAME_RE = re.compile(r"^(?:phase_)?graph_alignment_edge_(\d+)_(.+)$")
EQUALITY_SEED_NAME_RE = re.compile(
    r"^(?P<seed>.*equality_role_seed_\d+)_(?P<metric>"
    r"source_role|source_field|source_rows|target_role|target_field|target_rows_before|"
    r"target_rows_after|values|candidate_rows|applied|reason|elapsed"
    r")$"
)
EDGE_DESC_RE = re.compile(
    r"iter=(?P<iter>\d+)\s+edge=(?P<edge>\d+)\s+input=(?P<input>\d+)\s+"
    r"(?P<parent_role>[^:\s]+):(?P<parent_table>[^\[]+)\[(?P<parent_rows>\d+)\]\s+->\s+"
    r"(?P<child_role>[^:\s]+):(?P<child_table>[^\[]+)\[(?P<child_rows>\d+)\]"
)
KEY_VALUE_RE = re.compile(r"([A-Za-z0-9_]+)=([^\s]+)")

RELATIONSHIP_PHASE_NAMES = [
    "phase_execute_relationship_vector_elapsed",
    "phase_graph_reduction_elapsed",
    "phase_graph_grouped_aggregate_graph_reduction_elapsed",
    "phase_graph_grouped_aggregate_alignment_elapsed",
    "phase_graph_grouped_aggregate_materialization_elapsed",
    "phase_graph_grouped_aggregate_tuple_expansion_elapsed",
    "phase_graph_grouped_aggregate_same_row_elapsed",
    "phase_graph_grouped_aggregate_membership_elapsed",
    "phase_graph_grouped_aggregate_residual_filter_elapsed",
    "phase_graph_grouped_aggregate_execution_elapsed",
    "phase_graph_grouped_aggregate_preagg_materialization_elapsed",
    "phase_graph_grouped_aggregate_preagg_elapsed",
    "phase_graph_grouped_aggregate_preagg_storage_elapsed",
    "phase_graph_grouped_aggregate_preagg_storage_lookup_elapsed",
    "phase_graph_grouped_aggregate_preagg_storage_projection_elapsed",
    "phase_graph_grouped_aggregate_preagg_storage_aggregate_elapsed",
    "phase_graph_grouped_aggregate_group_row_build_elapsed",
    "phase_graph_grouped_aggregate_having_elapsed",
    "phase_graph_grouped_aggregate_final_materialization_elapsed",
    "phase_graph_grouped_aggregate_final_sort_elapsed",
    "phase_graph_grouped_aggregate_output_elapsed",
    "phase_graph_grouped_aggregate_final_limit_elapsed",
]

Q3_PHASE_NAMES = [
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
    "phase_graph_reduction_value_vector_column_ids_total_elapsed",
    "phase_graph_reduction_value_vector_read_total_elapsed",
    "phase_graph_reduction_value_vector_pair_total_elapsed",
    "phase_graph_reduction_batch_equal_total_elapsed",
    "phase_graph_reduction_intersect_total_elapsed",
    "phase_graph_reduction_pair_total_elapsed",
    "phase_graph_reduction_child_retain_total_elapsed",
]

PREAGG_COUNTER_NAMES = [
    "graph_sink_rows",
    "graph_reduction_edges_evaluated",
    "graph_reduction_parent_rows_seen",
    "graph_reduction_child_rows_seen",
    "graph_reduction_joined_rows_seen",
    "graph_reduction_reverse_artifact_candidate_rows",
    "graph_reduction_reverse_artifact_narrowed_rows",
    "graph_reduction_matched_rows",
    "graph_reduction_value_vector_child_rows",
    "graph_reduction_value_vector_values",
    "graph_reduction_value_vector_exists",
    "graph_reduction_value_vector_parent_misses",
    "graph_grouped_aggregate_rows",
    "graph_grouped_aggregate_fields",
    "graph_grouped_aggregate_materialization_rows",
    "graph_grouped_aggregate_materialization_fields",
    "graph_grouped_aggregate_residual_predicates",
    "graph_grouped_aggregate_residual_rows_before",
    "graph_grouped_aggregate_residual_rows_after",
    "graph_grouped_aggregate_residual_rows_removed",
    "graph_grouped_aggregate_preagg_rows",
    "graph_grouped_aggregate_preagg_groups",
    "graph_grouped_aggregate_post_having_groups",
    "graph_grouped_aggregate_final_materialization_rows",
    "graph_grouped_aggregate_final_materialization_fields",
    "graph_grouped_aggregate_preagg_storage_nodes",
    "graph_grouped_aggregate_preagg_storage_projection_shards_visited",
    "graph_grouped_aggregate_preagg_storage_projection_shards_retained",
    "graph_grouped_aggregate_preagg_storage_projection_rows_retained",
    "node_interaction_estimate_initial_row_reads",
    "node_interaction_estimate_vector_projection_reads",
    "node_interaction_estimate_materialization_reads",
    "node_interaction_estimate_total_reads",
    "q3_attribution_input_line_rows",
    "q3_attribution_input_order_rows",
    "q3_attribution_final_materialization_rows",
    "q3_attribution_output_rows",
]

POLICY_EVENT_NAMES = [
    "graph_equality_role_seed_enabled",
    "graph_equality_role_seed_mode",
    "graph_equality_role_seed_auto_max_source_rows",
    "graph_equality_role_seed_fields",
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
    position = 0
    total = 0.0
    matched = False
    for match in DURATION_RE.finditer(value):
        if match.start() != position:
            return None
        total += float(match.group(1)) * DURATION_UNITS[match.group(2)]
        position = match.end()
        matched = True
    if not matched or position != len(value):
        return None
    return total


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
    rows = []
    for name in names:
        values = timing_values(runs, name)
        if not values:
            continue
        rows.append(
            f"  {name}: median={fmt_seconds(median(values))} "
            f"p95={fmt_seconds(percentile(values, 0.95))} max={fmt_seconds(max(values))} n={len(values)}"
        )
    if not rows:
        return
    print(title)
    for row in rows:
        print(row)


def print_counter_table(title: str, runs: list[list[dict[str, Any]]], names: list[str]) -> None:
    rows = []
    for name in names:
        values = counter_values(runs, name)
        if not values:
            continue
        rows.append(f"  {name}: median={fmt_int(int(median(values)))} max={fmt_int(max(values))} n={len(values)}")
    if not rows:
        return
    print(title)
    for row in rows:
        print(row)


def field_probe_bases(runs: list[list[dict[str, Any]]]) -> list[str]:
    bases = set()
    for run in runs:
        for obj in run:
            name = str(obj.get("name") or "")
            for suffix in ("_fetch_elapsed", "_attach_elapsed"):
                if name.startswith("graph_") and name.endswith(suffix) and "_materialization_field_" in name:
                    bases.add(name[: -len(suffix)])
    return sorted(bases)


def first_event_value(runs: list[list[dict[str, Any]]], name: str) -> str:
    values = event_values(runs, name)
    return values[0] if values else ""


def print_materialization_summary(runs: list[list[dict[str, Any]]]) -> None:
    bases = field_probe_bases(runs)
    if not bases:
        return
    print("materialization_fields")
    rows = []
    for base in bases:
        fetch_values = timing_values(runs, f"{base}_fetch_elapsed")
        attach_values = timing_values(runs, f"{base}_attach_elapsed")
        total = median(fetch_values) + median(attach_values)
        rows.append((total, base, fetch_values, attach_values))
    for _, base, fetch_values, attach_values in sorted(rows, reverse=True):
        role = first_event_value(runs, f"{base}_role") or "?"
        table = first_event_value(runs, f"{base}_table") or "?"
        field = first_event_value(runs, f"{base}_field") or "?"
        source = first_event_value(runs, f"{base}_source")
        row_values = counter_values(runs, f"{base}_rows")
        parts = [
            f"{role}.{table}.{field}",
            f"rows={fmt_int(int(median(row_values))) if row_values else '-'}",
            f"fetch={fmt_seconds(median(fetch_values)) if fetch_values else '0s'}",
            f"attach={fmt_seconds(median(attach_values)) if attach_values else '0s'}",
        ]
        if source:
            parts.append(f"source={source}")
        print(f"  {' '.join(parts)}")


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


def metric_median(metrics: dict[str, list[Any]], name: str) -> float:
    return median([value for value in metrics.get(name, []) if isinstance(value, float)])


def metric_int_median(metrics: dict[str, list[Any]], name: str) -> int | None:
    values = [value for value in metrics.get(name, []) if isinstance(value, int)]
    if not values:
        return None
    return int(median(values))


def print_edge_summary(runs: list[list[dict[str, Any]]]) -> None:
    edges = edge_entries(runs)
    print("edges_by_reduce")
    sortable = []
    for key, metrics in edges.items():
        reduce_median = metric_median(metrics, "reduce_elapsed") or metric_median(metrics, "summary_reduce")
        sortable.append((reduce_median, key, metrics))
    for reduce_median, key, metrics in sorted(sortable, reverse=True):
        desc = scalar_median(metrics.get("edge_desc", [])) or f"iter={key[0]} edge={key[1]}"
        parts = [f"reduce={fmt_seconds(reduce_median)}"]
        for metric in (
            "reverse_artifact_elapsed",
            "reverse_artifact_client_rpc_elapsed",
            "reverse_artifact_client_rpc_max_elapsed",
            "reverse_artifact_response_merge_elapsed",
            "reverse_artifact_row_merge_elapsed",
            "reverse_artifact_parent_merge_elapsed",
            "reverse_artifact_sort_elapsed",
            "reverse_artifact_source_elapsed",
            "reverse_artifact_read_request_elapsed",
            "reverse_artifact_row_conversion_elapsed",
            "reverse_artifact_map_conversion_elapsed",
            "reverse_artifact_narrow_elapsed",
            "reverse_artifact_parent_map_elapsed",
            "reverse_artifact_projection_intersect_elapsed",
            "parent_key_elapsed",
            "child_retain_elapsed",
            "value_vector_elapsed",
            "value_vector_column_ids_elapsed",
            "value_vector_read_elapsed",
            "value_vector_pair_elapsed",
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
            "value_vector_child_rows",
            "value_vector_values",
            "value_vector_exists",
            "value_vector_parent_misses",
        ):
            values = [v for v in metrics.get(metric, []) if isinstance(v, int)]
            if values:
                parts.append(f"{metric}={fmt_int(int(median(values)))}")
        parent_rows = median([v for v in metrics.get("parent_rows", []) if isinstance(v, int)])
        child_rows = median([v for v in metrics.get("child_rows", []) if isinstance(v, int)])
        joined_rows = median([v for v in metrics.get("joined_rows", []) if isinstance(v, int)])
        candidate_rows = median([v for v in metrics.get("reverse_artifact_candidate_rows", []) if isinstance(v, int)])
        value_vector_values = median([v for v in metrics.get("value_vector_values", []) if isinstance(v, int)])
        value_vector_exists = median([v for v in metrics.get("value_vector_exists", []) if isinstance(v, int)])
        value_vector_parent_misses = median([v for v in metrics.get("value_vector_parent_misses", []) if isinstance(v, int)])
        if parent_rows:
            parts.append(f"fanout={joined_rows / parent_rows:.2f}x")
        if child_rows:
            parts.append(f"child_selectivity={joined_rows / child_rows:.4f}")
        if joined_rows:
            parts.append(f"candidate_overjoin={candidate_rows / joined_rows:.2f}x")
        if value_vector_values:
            parent_hits = max(0, value_vector_exists - value_vector_parent_misses)
            parts.append(f"value_vector_parent_hit_rate={parent_hits / value_vector_values:.4f}")
        mode = scalar_median(metrics.get("value_vector_mode", []))
        if mode:
            parts.append(f"value_vector_mode={mode}")
        ra_mode = scalar_median(metrics.get("reverse_artifact_mode", []))
        if ra_mode:
            parts.append(f"reverse_artifact_mode={ra_mode}")
        local_mode = scalar_median(metrics.get("reverse_artifact_local_mode", []))
        if local_mode:
            parts.append(f"reverse_artifact_local_mode={local_mode}")
        target_mode = scalar_median(metrics.get("reverse_artifact_target_candidate_mode", []))
        if target_mode:
            parts.append(f"reverse_artifact_target_candidate_mode={target_mode}")
        print(f"  {' '.join(parts)}  {desc}")


def print_shared_child_summary(runs: list[list[dict[str, Any]]]) -> None:
    edges = edge_entries(runs)
    groups: dict[tuple[str, str], list[dict[str, list[Any]]]] = defaultdict(list)
    for metrics in edges.values():
        child_role = scalar_median(metrics.get("child_role", []))
        child_table = scalar_median(metrics.get("child_table", []))
        if not child_role or not child_table:
            continue
        groups[(str(child_role), str(child_table))].append(metrics)
    rows = []
    for (child_role, child_table), group_edges in groups.items():
        if len(group_edges) < 2:
            continue
        reverse_artifact_total = sum(
            metric_median(metrics, "reverse_artifact_elapsed") or metric_median(metrics, "summary_reverse_artifact")
            for metrics in group_edges
        )
        rpc_max_total = sum(
            metric_median(metrics, "reverse_artifact_client_rpc_max_elapsed")
            or metric_median(metrics, "summary_reverse_artifact_client_rpc_max")
            for metrics in group_edges
        )
        response_merge_total = sum(
            metric_median(metrics, "reverse_artifact_response_merge_elapsed")
            or metric_median(metrics, "summary_reverse_artifact_response_merge")
            for metrics in group_edges
        )
        narrowed_total = sum(metric_int_median(metrics, "reverse_artifact_narrowed_rows") or 0 for metrics in group_edges)
        joined_total = sum(metric_int_median(metrics, "joined_rows") or 0 for metrics in group_edges)
        rows.append(
            (
                reverse_artifact_total,
                child_role,
                child_table,
                group_edges,
                rpc_max_total,
                response_merge_total,
                narrowed_total,
                joined_total,
            )
        )
    if not rows:
        return
    print("shared_child_hotspots")
    for total, child_role, child_table, group_edges, rpc_max, response_merge, narrowed_total, joined_total in sorted(rows, reverse=True):
        parents = []
        for metrics in group_edges:
            parent_role = scalar_median(metrics.get("parent_role", [])) or "?"
            parent_table = scalar_median(metrics.get("parent_table", [])) or "?"
            parents.append(f"{parent_role}:{parent_table}")
        print(
            f"  child={child_role}:{child_table} incoming={len(group_edges)} "
            f"parents={','.join(parents)} reverse_artifact_total={fmt_seconds(total)} "
            f"rpc_max_total={fmt_seconds(rpc_max)} response_merge_total={fmt_seconds(response_merge)} "
            f"joined_rows_total={fmt_int(joined_total)} narrowed_rows_total={fmt_int(narrowed_total)}"
        )


def alignment_edge_entries(runs: list[list[dict[str, Any]]]) -> dict[int, dict[str, list[Any]]]:
    edges: dict[int, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
    for run in runs:
        for obj in run:
            name = str(obj.get("name") or "")
            value = object_value(obj)
            match = ALIGNMENT_EDGE_NAME_RE.match(name)
            if not match:
                continue
            edge_id = int(match.group(1))
            metric = match.group(2)
            seconds = duration_seconds(value)
            parsed_int = int_value(value)
            if seconds is not None:
                edges[edge_id][metric].append(seconds)
            elif parsed_int is not None:
                edges[edge_id][metric].append(parsed_int)
            elif value != "":
                edges[edge_id][metric].append(value)
    return edges


def print_alignment_summary(runs: list[list[dict[str, Any]]]) -> None:
    edges = alignment_edge_entries(runs)
    if not edges:
        return
    rows = []
    for edge_id, metrics in edges.items():
        elapsed = metric_median(metrics, "elapsed")
        rows.append((elapsed, edge_id, metrics))
    print("alignment_edges")
    for elapsed, edge_id, metrics in sorted(rows, reverse=True):
        source = scalar_median(metrics.get("source", [])) or "?"
        parent_role = scalar_median(metrics.get("parent_role", [])) or "?"
        parent_table = scalar_median(metrics.get("parent_table", [])) or "?"
        child_role = scalar_median(metrics.get("child_role", [])) or "?"
        child_table = scalar_median(metrics.get("child_table", [])) or "?"
        child_rows = metric_int_median(metrics, "child_rows")
        parent_rows = metric_int_median(metrics, "parent_rows")
        print(
            f"  edge={edge_id} elapsed={fmt_seconds(elapsed)} source={source} "
            f"{parent_role}:{parent_table}->{child_role}:{child_table} "
            f"child_rows={fmt_int(child_rows)} parent_rows={fmt_int(parent_rows)}"
        )


def print_policy_summary(runs: list[list[dict[str, Any]]]) -> None:
    print("policy")
    for name in POLICY_EVENT_NAMES:
        values = event_values(runs, name)
        if not values:
            continue
        counts = Counter(values)
        rendered = ", ".join(f"{value or '<empty>'}:{count}" for value, count in counts.most_common())
        print(f"  {name}: {rendered}")


def equality_seed_entries(runs: list[list[dict[str, Any]]]) -> dict[str, dict[str, list[Any]]]:
    seeds: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
    for run in runs:
        for obj in run:
            name = str(obj.get("name") or "")
            match = EQUALITY_SEED_NAME_RE.match(name)
            if not match:
                continue
            seed = match.group("seed")
            metric = match.group("metric")
            value = object_value(obj)
            seconds = duration_seconds(value)
            parsed_int = int_value(value)
            if seconds is not None:
                seeds[seed][metric].append(seconds)
            elif parsed_int is not None:
                seeds[seed][metric].append(parsed_int)
            elif value != "":
                seeds[seed][metric].append(value)
    return seeds


def print_equality_seed_summary(runs: list[list[dict[str, Any]]]) -> None:
    seeds = equality_seed_entries(runs)
    if not seeds:
        return
    print("equality_seeds")
    for seed, metrics in sorted(seeds.items()):
        source_role = scalar_median(metrics.get("source_role", [])) or "?"
        source_field = scalar_median(metrics.get("source_field", [])) or "?"
        target_role = scalar_median(metrics.get("target_role", [])) or "?"
        target_field = scalar_median(metrics.get("target_field", [])) or "?"
        applied = scalar_median(metrics.get("applied", []))
        reason = scalar_median(metrics.get("reason", []))
        elapsed = [v for v in metrics.get("elapsed", []) if isinstance(v, float)]
        parts = [
            f"{seed}",
            f"{source_role}.{source_field}->{target_role}.{target_field}",
        ]
        for metric in ("source_rows", "values", "candidate_rows", "target_rows_before", "target_rows_after"):
            values = [v for v in metrics.get(metric, []) if isinstance(v, int)]
            if values:
                parts.append(f"{metric}={fmt_int(int(median(values)))}")
        if applied is not None:
            parts.append(f"applied={applied}")
        if reason:
            parts.append(f"reason={reason}")
        if elapsed:
            parts.append(f"elapsed={fmt_seconds(median(elapsed))}")
        print(f"  {' '.join(parts)}")


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
    print_timing_table("relationship_phase_timings", runs, RELATIONSHIP_PHASE_NAMES)
    print_timing_table("q3_phase_timings", runs, Q3_PHASE_NAMES)
    print_timing_table("graph_reduction_totals", runs, REDUCTION_TOTAL_NAMES)
    print_counter_table("relationship_rows", runs, PREAGG_COUNTER_NAMES)
    print_materialization_summary(runs)
    print_edge_summary(runs)
    print_shared_child_summary(runs)
    print_alignment_summary(runs)
    print_equality_seed_summary(runs)
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
