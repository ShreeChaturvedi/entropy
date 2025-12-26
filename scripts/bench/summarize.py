#!/usr/bin/env python3
import csv
import json
import sys
from typing import Dict, Tuple


def to_ns(value: float, unit: str) -> float:
    if unit == "ns":
        return value
    if unit == "us":
        return value * 1_000.0
    if unit == "ms":
        return value * 1_000_000.0
    if unit == "s":
        return value * 1_000_000_000.0
    raise ValueError(f"Unsupported time unit: {unit}")


def parse_name(name: str) -> Tuple[str, int, str]:
    base, _, arg = name.partition("/")
    rows = int(arg) if arg else 0

    if "Entropy" in base:
        engine = "entropy"
    elif "Sqlite" in base:
        engine = "sqlite"
    else:
        engine = "other"

    if "InsertBatch" in base:
        case = "insert_batch"
    elif "PointSelect" in base:
        case = "point_select"
    else:
        case = base

    return case, rows, engine


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: summarize.py <benchmark.json> <output.csv>")
        return 1

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    with open(input_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    summary: Dict[Tuple[str, int], Dict[str, float]] = {}
    for entry in data.get("benchmarks", []):
        if entry.get("aggregate_name") or entry.get("run_type") == "aggregate":
            continue

        name = entry.get("name", "")
        if not name:
            continue

        case, rows, engine = parse_name(name)
        if engine == "other":
            continue

        real_time = float(entry.get("real_time", 0.0))
        unit = entry.get("time_unit", "ns")
        ns_per_op = to_ns(real_time, unit)

        key = (case, rows)
        summary.setdefault(key, {})[engine] = ns_per_op

    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["case", "rows", "entropy_ns_per_op", "sqlite_ns_per_op", "ratio"])
        for (case, rows) in sorted(summary.keys()):
            row = summary[(case, rows)]
            entropy_ns = row.get("entropy", "")
            sqlite_ns = row.get("sqlite", "")

            ratio = ""
            if isinstance(entropy_ns, float) and isinstance(sqlite_ns, float) and sqlite_ns != 0.0:
                ratio = entropy_ns / sqlite_ns

            writer.writerow([case, rows, entropy_ns, sqlite_ns, ratio])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
