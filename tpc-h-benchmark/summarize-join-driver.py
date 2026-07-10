#!/usr/bin/env python3
import re
import sys
from collections import Counter
from pathlib import Path


SELECTED_RE = re.compile(r"JOIN_DRIVER selected=(?P<driver>\S+).* found_sets=(?P<sets>.*)$")
POST_RE = re.compile(r"JOIN_DRIVER post_reduce selected=(?P<driver>\S+) found_sets=(?P<sets>.*)$")
CANDIDATE_RE = re.compile(
    r"JOIN_DRIVER_CANDIDATE rank=(?P<rank>\d+) table=(?P<table>\S+) "
    r"selected=(?P<selected>\S+) count=(?P<count>\d+) "
    r"child_edges=(?P<child_edges>\d+) parent_edges=(?P<parent_edges>\d+) "
    r"legality=(?P<legality>\S+) "
    r"(?:expansion_status=(?P<expansion_status>\S+) expanded_sets=(?P<expanded_sets>\S+) )?"
    r"found_sets=(?P<sets>.*)$"
)
COST_RE = re.compile(
    r"JOIN_DRIVER_COST rank=(?P<rank>\d+) table=(?P<table>\S+) "
    r"selected=(?P<selected>\S+) status=(?P<status>\S+) score=(?P<score>\d+) "
    r"expanded_rows=(?P<expanded_rows>\d+) projection_work=(?P<projection_work>\d+) "
    r"relation_work=(?P<relation_work>\d+) projected_fields=(?P<projected_fields>\d+) "
    r"relation_fields=(?P<relation_fields>\d+) field_counts=(?P<field_counts>\S+) "
    r"expanded_sets=(?P<expanded_sets>.*)$"
)

# Approximate parent-to-child depth for the current benchmark and QA schemas.
# The summary uses this only as a diagnostic hint; it does not declare optimizer
# legality for arbitrary user schemas.
TABLE_DEPTH = {
    "region": 0,
    "nation": 1,
    "customer": 2,
    "supplier": 2,
    "part": 2,
    "orders": 3,
    "partsupp": 3,
    "lineitem": 4,
    "customers_qa": 0,
    "orders_qa": 1,
    "lineitems_qa": 2,
    "deliveries_qa": 3,
}


def parse_found_sets(text):
    result = {}
    for part in text.split(","):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        try:
            result[key] = int(value)
        except ValueError:
            continue
    return result


def smaller_candidates(driver, found_sets):
    driver_count = found_sets.get(driver)
    if driver_count is None:
        return []
    return sorted(
        (table, count) for table, count in found_sets.items() if table != driver and count < driver_count
    )


def format_found_sets(found_sets):
    return ",".join(f"{table}={count}" for table, count in sorted(found_sets.items()))


def table_depth(table):
    return TABLE_DEPTH.get(table)


def deepest_tables(found_sets):
    known = [(table, table_depth(table)) for table in found_sets]
    known = [(table, depth) for table, depth in known if depth is not None]
    if not known:
        return set()
    max_depth = max(depth for _, depth in known)
    return {table for table, depth in known if depth == max_depth}


def legality_hint(driver, found_sets):
    deepest = deepest_tables(found_sets)
    if not deepest:
        return "unknown-schema"
    if driver in deepest:
        return "deepest-fk-side"
    return "not-deepest"


def parent_side_smaller_candidates(driver, found_sets):
    driver_depth = table_depth(driver)
    if driver_depth is None:
        return smaller_candidates(driver, found_sets)
    result = []
    for table, count in smaller_candidates(driver, found_sets):
        depth = table_depth(table)
        if depth is None or depth < driver_depth:
            result.append((table, count))
    return result


def summarize(log_path):
    selected = Counter()
    post = Counter()
    smaller_post = Counter()
    post_rows = []
    candidate_legality = Counter()
    selected_candidate_legality = Counter()
    candidate_rankings = Counter()
    candidate_expansion_status = Counter()
    cost_status = Counter()
    cost_rankings = Counter()

    for line in log_path.read_text(errors="replace").splitlines():
        selected_match = SELECTED_RE.search(line)
        if selected_match and "post_reduce" not in line:
            driver = selected_match.group("driver")
            found_sets = parse_found_sets(selected_match.group("sets"))
            selected[(driver, format_found_sets(found_sets))] += 1
            continue

        post_match = POST_RE.search(line)
        if post_match:
            driver = post_match.group("driver")
            found_sets = parse_found_sets(post_match.group("sets"))
            found_sets_text = format_found_sets(found_sets)
            post[(driver, found_sets_text)] += 1
            smaller = parent_side_smaller_candidates(driver, found_sets)
            if smaller:
                smaller_post[(driver, found_sets_text, tuple(smaller))] += 1
            post_rows.append((driver, found_sets))
            continue

        candidate_match = CANDIDATE_RE.search(line)
        if candidate_match:
            rank = int(candidate_match.group("rank"))
            table = candidate_match.group("table")
            count = int(candidate_match.group("count"))
            legality = candidate_match.group("legality")
            expansion_status = candidate_match.group("expansion_status") or "not-recorded"
            expanded_sets = candidate_match.group("expanded_sets") or ""
            found_sets = parse_found_sets(candidate_match.group("sets"))
            found_sets_text = format_found_sets(found_sets)
            selected_candidate = candidate_match.group("selected") == "true"
            candidate_legality[legality] += 1
            candidate_expansion_status[expansion_status] += 1
            if selected_candidate:
                selected_candidate_legality[legality] += 1
            candidate_rankings[
                (rank, table, count, legality, selected_candidate, expansion_status, expanded_sets, found_sets_text)
            ] += 1
            continue

        cost_match = COST_RE.search(line)
        if cost_match:
            rank = int(cost_match.group("rank"))
            table = cost_match.group("table")
            selected_candidate = cost_match.group("selected") == "true"
            status = cost_match.group("status")
            score = int(cost_match.group("score"))
            expanded_rows = int(cost_match.group("expanded_rows"))
            projection_work = int(cost_match.group("projection_work"))
            relation_work = int(cost_match.group("relation_work"))
            projected_fields = int(cost_match.group("projected_fields"))
            relation_fields = int(cost_match.group("relation_fields"))
            field_counts = cost_match.group("field_counts")
            expanded_sets = cost_match.group("expanded_sets")
            cost_status[status] += 1
            cost_rankings[
                (
                    rank,
                    table,
                    selected_candidate,
                    status,
                    score,
                    expanded_rows,
                    projection_work,
                    relation_work,
                    projected_fields,
                    relation_fields,
                    field_counts,
                    expanded_sets,
                )
            ] += 1

    print(f"JOIN_DRIVER summary: {log_path}")
    print(
        f"selected_events={sum(selected.values())} "
        f"post_reduce_events={sum(post.values())} "
        f"candidate_events={sum(candidate_legality.values())} "
        f"cost_events={sum(cost_status.values())}"
    )
    print()

    print("post-reduce driver choices:")
    print(
        f"{'runs':>4} {'driver':14} {'driver_count':>12} {'legality_hint':18} "
        f"{'smallest_table':18} {'smallest_count':>14}  found_sets"
    )
    print(
        f"{'-' * 4:>4} {'-' * 14:14} {'-' * 12:>12} {'-' * 18:18} "
        f"{'-' * 18:18} {'-' * 14:>14}  {'-' * 40}"
    )
    for (driver, found_sets_text), runs in post.most_common():
        found_sets = parse_found_sets(found_sets_text)
        driver_count = found_sets.get(driver, 0)
        smallest_table, smallest_count = min(found_sets.items(), key=lambda item: item[1])
        print(
            f"{runs:>4} {driver[:14]:14} {driver_count:>12} "
            f"{legality_hint(driver, found_sets)[:18]:18} "
            f"{smallest_table[:18]:18} {smallest_count:>14}  {found_sets_text}"
        )

    if smaller_post:
        print()
        print("post-reduce cases with smaller parent-side candidate:")
        parent_column_width = 64
        print(f"{'runs':>4} {'driver':14} {'smaller_parent_side_candidates':{parent_column_width}}  found_sets")
        print(f"{'-' * 4:>4} {'-' * 14:14} {'-' * parent_column_width:{parent_column_width}}  {'-' * 40}")
        for (driver, found_sets_text, smaller), runs in smaller_post.most_common():
            smaller_text = ",".join(f"{table}={count}" for table, count in smaller)
            smaller_display = smaller_text[:parent_column_width]
            print(f"{runs:>4} {driver[:14]:14} {smaller_display:{parent_column_width}}  {found_sets_text}")

    if candidate_legality:
        print()
        print("candidate legality counts:")
        print(f"{'events':>6} {'legality':34} {'selected_events':>15}")
        print(f"{'-' * 6:>6} {'-' * 34:34} {'-' * 15:>15}")
        for legality, events in candidate_legality.most_common():
            print(f"{events:>6} {legality[:34]:34} {selected_candidate_legality[legality]:>15}")

        print()
        print("candidate expansion status counts:")
        print(f"{'events':>6} {'expansion_status':50}")
        print(f"{'-' * 6:>6} {'-' * 50:50}")
        for expansion_status, events in candidate_expansion_status.most_common():
            print(f"{events:>6} {expansion_status[:50]:50}")

        print()
        print("candidate ranking samples:")
        print(
            f"{'runs':>4} {'rank':>4} {'table':14} {'count':>12} {'selected':>8} "
            f"{'legality':34} {'expansion_status':24}  expanded_sets"
        )
        print(
            f"{'-' * 4:>4} {'-' * 4:>4} {'-' * 14:14} {'-' * 12:>12} {'-' * 8:>8} "
            f"{'-' * 34:34} {'-' * 24:24}  {'-' * 40}"
        )
        for (
            rank,
            table,
            count,
            legality,
            selected_candidate,
            expansion_status,
            expanded_sets,
            _found_sets_text,
        ), runs in candidate_rankings.most_common(16):
            print(
                f"{runs:>4} {rank:>4} {table[:14]:14} {count:>12} "
                f"{str(selected_candidate).lower():>8} {legality[:34]:34} "
                f"{expansion_status[:24]:24}  {expanded_sets[:80]}"
            )

    if cost_status:
        print()
        print("candidate cost status counts:")
        print(f"{'events':>6} {'status':50}")
        print(f"{'-' * 6:>6} {'-' * 50:50}")
        for status, events in cost_status.most_common():
            print(f"{events:>6} {status[:50]:50}")

        print()
        print("candidate cost samples:")
        print(
            f"{'runs':>4} {'rank':>4} {'table':14} {'selected':>8} {'score':>12} "
            f"{'expanded':>10} {'proj_work':>10} {'rel_work':>10} {'fields':>6} {'rels':>4}  field_counts"
        )
        print(
            f"{'-' * 4:>4} {'-' * 4:>4} {'-' * 14:14} {'-' * 8:>8} {'-' * 12:>12} "
            f"{'-' * 10:>10} {'-' * 10:>10} {'-' * 10:>10} {'-' * 6:>6} {'-' * 4:>4}  {'-' * 40}"
        )
        for (
            rank,
            table,
            selected_candidate,
            status,
            score,
            expanded_rows,
            projection_work,
            relation_work,
            projected_fields,
            relation_fields,
            field_counts,
            _expanded_sets,
        ), runs in cost_rankings.most_common(16):
            print(
                f"{runs:>4} {rank:>4} {table[:14]:14} {str(selected_candidate).lower():>8} "
                f"{score:>12} {expanded_rows:>10} {projection_work:>10} {relation_work:>10} "
                f"{projected_fields:>6} {relation_fields:>4}  {field_counts[:80]}"
            )

    print()
    print("selected transition samples:")
    for (driver, found_sets_text), runs in selected.most_common(12):
        print(f"{runs:>4} driver={driver} found_sets={found_sets_text}")


def main():
    if len(sys.argv) != 2:
        print("usage: summarize-join-driver.py <join-driver-log>", file=sys.stderr)
        return 2
    log_path = Path(sys.argv[1])
    if not log_path.is_file():
        print(f"log file not found: {log_path}", file=sys.stderr)
        return 2
    summarize(log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
