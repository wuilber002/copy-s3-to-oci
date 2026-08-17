from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_language_selector_is_available_only_in_interface_settings():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert page.count('id="language-selector"') == 1
    assert page.index('id="view-settings"') < page.index('id="language-selector"')
    assert 'id="set-multipart-part-size"' in page


def test_translation_catalog_covers_multipart_dynamic_feedback_and_restore_queue():
    catalog = (ROOT / "app/static/i18n.js").read_text(encoding="utf-8")
    for phrase in ("Multipart upload", "OCI destination validated", "Missing example:", "Interface language", "Batch job:", "Next attempt:"):
        assert phrase in catalog
    assert "node.nodeType === Node.TEXT_NODE" in catalog
