from __future__ import annotations

import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "generate_coverage_badge.py"
SPEC = importlib.util.spec_from_file_location("generate_coverage_badge", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_generate_coverage_badge_script(tmp_path, monkeypatch):
    good_coverage_xml = tmp_path / "coverage-good.xml"
    warning_coverage_xml = tmp_path / "coverage-warning.xml"
    output_svg = tmp_path / "coverage.svg"
    good_coverage_xml.write_text(
        '<?xml version="1.0" ?><coverage version="7.0.0" line-rate="0.9511" branch-rate="0.0"></coverage>',
        encoding="utf-8",
    )
    warning_coverage_xml.write_text(
        '<?xml version="1.0" ?><coverage version="7.0.0" line-rate="0.7425" branch-rate="0.0"></coverage>',
        encoding="utf-8",
    )

    good_metadata = MODULE.build_badge_metadata(good_coverage_xml)
    warning_metadata = MODULE.build_badge_metadata(warning_coverage_xml)
    good_svg = MODULE.build_badge_svg(good_coverage_xml)
    warning_svg = MODULE.build_badge_svg(warning_coverage_xml)

    assert good_metadata == {
        "label": "coverage",
        "message": "95.11%",
        "color": "brightgreen",
        "color_hex": "#4c1",
    }
    assert warning_metadata == {
        "label": "coverage",
        "message": "74.25%",
        "color": "yellow",
        "color_hex": "#dfb317",
    }
    assert "coverage: 95.11%" in good_svg
    assert "#4c1" in good_svg
    assert "coverage: 74.25%" in warning_svg
    assert "#dfb317" in warning_svg

    monkeypatch.setattr(
        "sys.argv",
        ["generate_coverage_badge.py", str(good_coverage_xml), str(output_svg)],
    )

    assert MODULE.main() == 0
    saved_svg = output_svg.read_text(encoding="utf-8")
    assert saved_svg == good_svg
    assert "95.11%" in saved_svg
