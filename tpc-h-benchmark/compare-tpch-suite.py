#!/usr/bin/env python3
import re
import statistics
import sys
from pathlib import Path


RESULT_RE = re.compile(
    r"\b(?P<status>PASS|FAIL|XFAIL|XPASS|SKIP)\s+"
    r"(?P<id>[A-Za-z0-9_][A-Za-z0-9_.-]*\.[A-Za-z0-9_.-]+)"
    r"(?:\s+\[(?P<duration>[^\]]+)\])?"
)


def parse_duration(text):
    if not text:
        return None
    text = text.strip()
    if text == "<1s":
        return 0.0
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


def format_ratio(baseline, candidate):
    if baseline is None or candidate is None:
        return "-"
    if baseline == 0:
        return "-" if candidate == 0 else "inf"
    return f"{candidate / baseline:.2f}x"


def parse_label(line, label):
    if line.startswith("engine="):
        return line.split("=", 1)[1].strip() or label
    if line.startswith("profile="):
        return line.split("=", 1)[1].strip() or label
    return label


def parse_log(path):
    label = path.stem
    cases = {}

    for line in path.read_text(errors="replace").splitlines():
        label = parse_label(line, label)
        result = RESULT_RE.search(line)
        if not result:
            continue
        case_id = result.group("id")
        duration = parse_duration(result.group("duration"))
        entry = cases.setdefault(case_id, {"statuses": [], "durations": []})
        entry["statuses"].append(result.group("status"))
        if duration is not None:
            entry["durations"].append(duration)

    for entry in cases.values():
        durations = entry["durations"]
        entry["median"] = statistics.median(durations) if durations else None
        entry["status"] = ",".join(sorted(set(entry["statuses"]))) if entry["statuses"] else "-"
    return {"path": path, "label": label, "cases": cases}


def column_widths(rows, headers):
    widths = [len(header) for header in headers]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))
    return widths


def print_table(headers, rows):
    widths = column_widths(rows, headers)
    print("  ".join(header.ljust(widths[i]) for i, header in enumerate(headers)))
    print("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in rows:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))


def compare(logs):
    baseline = logs[0]
    candidates = logs[1:]
    case_ids = sorted(
        set().union(*(set(log["cases"].keys()) for log in logs)),
        key=lambda case_id: (
            baseline["cases"].get(case_id, {}).get("median") is None,
            -(baseline["cases"].get(case_id, {}).get("median") or 0),
            case_id,
        ),
    )

    print("TPC-H suite timing comparison")
    print(f"baseline: {baseline['label']} ({baseline['path']})")
    for candidate in candidates:
        print(f"compare:  {candidate['label']} ({candidate['path']})")
    print()

    headers = ["case", f"{baseline['label']} status", f"{baseline['label']} median"]
    for candidate in candidates:
        headers.extend([f"{candidate['label']} status", f"{candidate['label']} median", "ratio"])

    rows = []
    for case_id in case_ids:
        base_entry = baseline["cases"].get(case_id, {})
        base_median = base_entry.get("median")
        row = [
            case_id,
            base_entry.get("status", "-"),
            format_duration(base_median),
        ]
        for candidate in candidates:
            entry = candidate["cases"].get(case_id, {})
            median = entry.get("median")
            row.extend([
                entry.get("status", "-"),
                format_duration(median),
                format_ratio(base_median, median),
            ])
        rows.append(row)

    print_table(headers, rows)


def main():
    if len(sys.argv) < 3:
        print("usage: compare-tpch-suite.py <baseline-log> <candidate-log> [candidate-log...]", file=sys.stderr)
        return 2
    paths = [Path(arg) for arg in sys.argv[1:]]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        print("log file not found: " + ", ".join(missing), file=sys.stderr)
        return 2
    compare([parse_log(path) for path in paths])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
