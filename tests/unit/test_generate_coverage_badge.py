from __future__ import annotations

import importlib.util
import json
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "generate_coverage_badge.py"
SPEC = importlib.util.spec_from_file_location("generate_coverage_badge", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_generate_coverage_badge_script(tmp_path, monkeypatch):
    coverage_xml = tmp_path / "coverage.xml"
    output_json = tmp_path / "badge.json"
    coverage_xml.write_text(
        '<?xml version="1.0" ?><coverage version="7.0.0" line-rate="0.9511" branch-rate="0.0"></coverage>',
        encoding="utf-8",
    )

    payload = MODULE.build_badge_payload(coverage_xml)
    assert payload == {
        "schemaVersion": 1,
        "label": "coverage",
        "message": "95.11%",
        "color": "brightgreen",
    }

    monkeypatch.setattr(
        "sys.argv",
        ["generate_coverage_badge.py", str(coverage_xml), str(output_json)],
    )

    assert MODULE.main() == 0
    assert json.loads(output_json.read_text(encoding="utf-8")) == payload
