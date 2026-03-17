"""Generate a Shields-compatible coverage badge from a coverage XML report."""

from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path


def _badge_color(coverage: float) -> str:
    if coverage >= 95:
        return "brightgreen"
    if coverage >= 90:
        return "green"
    if coverage >= 80:
        return "yellowgreen"
    if coverage >= 70:
        return "yellow"
    if coverage >= 60:
        return "orange"
    return "red"


def _coverage_percent(report_path: Path) -> float:
    root = ET.parse(report_path).getroot()
    line_rate = root.attrib.get("line-rate")
    if line_rate is None:
        msg = f"Coverage report {report_path} is missing the line-rate attribute."
        raise ValueError(msg)
    return round(float(line_rate) * 100, 2)


def build_badge_payload(report_path: Path) -> dict[str, object]:
    coverage = _coverage_percent(report_path)
    return {
        "schemaVersion": 1,
        "label": "coverage",
        "message": f"{coverage:.2f}%",
        "color": _badge_color(coverage),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage_xml", type=Path, help="Path to a coverage.xml file")
    parser.add_argument("output_json", type=Path, help="Path to write the badge JSON")
    args = parser.parse_args()

    payload = build_badge_payload(args.coverage_xml)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(f"{json.dumps(payload, indent=2)}\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
