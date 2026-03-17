"""Generate a simple SVG coverage badge from a coverage XML report."""

from __future__ import annotations

import argparse
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


def _badge_color_hex(color_name: str) -> str:
    palette = {
        "brightgreen": "#4c1",
        "green": "#97ca00",
        "yellowgreen": "#a4a61d",
        "yellow": "#dfb317",
        "orange": "#fe7d37",
        "red": "#e05d44",
    }
    return palette[color_name]


def _coverage_percent(report_path: Path) -> float:
    root = ET.parse(report_path).getroot()
    line_rate = root.attrib.get("line-rate")
    if line_rate is None:
        msg = f"Coverage report {report_path} is missing the line-rate attribute."
        raise ValueError(msg)
    return round(float(line_rate) * 100, 2)


def build_badge_metadata(report_path: Path) -> dict[str, str]:
    coverage = _coverage_percent(report_path)
    color_name = _badge_color(coverage)
    return {
        "label": "coverage",
        "message": f"{coverage:.2f}%",
        "color": color_name,
        "color_hex": _badge_color_hex(color_name),
    }


def _badge_width(text: str) -> int:
    return 10 + len(text) * 7


def build_badge_svg(report_path: Path) -> str:
    metadata = build_badge_metadata(report_path)
    label = metadata["label"]
    message = metadata["message"]
    color = metadata["color_hex"]
    label_width = _badge_width(label)
    message_width = _badge_width(message)
    total_width = label_width + message_width
    label_center = label_width / 2
    message_center = label_width + message_width / 2

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20" role="img" '
        f'aria-label="{label}: {message}">'
        "<title>"
        f"{label}: {message}"
        "</title>"
        '<linearGradient id="s" x2="0" y2="100%">'
        '<stop offset="0" stop-color="#fff" stop-opacity=".7"/>'
        '<stop offset=".1" stop-color="#aaa" stop-opacity=".1"/>'
        '<stop offset=".9" stop-opacity=".3"/>'
        '<stop offset="1" stop-opacity=".5"/>'
        "</linearGradient>"
        '<clipPath id="r"><rect width="'
        f"{total_width}"
        '" height="20" rx="3" fill="#fff"/></clipPath>'
        '<g clip-path="url(#r)">'
        f'<rect width="{label_width}" height="20" fill="#555"/>'
        f'<rect x="{label_width}" width="{message_width}" height="20" fill="{color}"/>'
        f'<rect width="{total_width}" height="20" fill="url(#s)"/>'
        "</g>"
        '<g fill="#fff" text-anchor="middle" '
        'font-family="Verdana,Geneva,DejaVu Sans,sans-serif" text-rendering="geometricPrecision" '
        'font-size="11">'
        f'<text x="{label_center}" y="15" fill="#010101" fill-opacity=".3">{label}</text>'
        f'<text x="{label_center}" y="14">{label}</text>'
        f'<text x="{message_center}" y="15" fill="#010101" fill-opacity=".3">{message}</text>'
        f'<text x="{message_center}" y="14">{message}</text>'
        "</g>"
        "</svg>\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage_xml", type=Path, help="Path to a coverage.xml file")
    parser.add_argument("output_svg", type=Path, help="Path to write the SVG badge")
    args = parser.parse_args()

    badge_svg = build_badge_svg(args.coverage_xml)
    args.output_svg.parent.mkdir(parents=True, exist_ok=True)
    args.output_svg.write_text(badge_svg, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
