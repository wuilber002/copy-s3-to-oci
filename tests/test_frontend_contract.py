from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_language_selector_is_available_only_in_interface_settings():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert page.count('id="language-selector"') == 1
    assert page.index('id="view-settings"') < page.index('id="language-selector"')
    assert 'id="set-multipart-part-size"' in page
    assert 'id="aws-connection"' in page
    assert 'id="aws-connection-secret"' in page


def test_translation_catalog_covers_multipart_dynamic_feedback_and_restore_queue():
    catalog = (ROOT / "app/static/i18n.js").read_text(encoding="utf-8")
    for phrase in ("Multipart upload", "OCI destination validated", "Missing example:", "Interface language", "Batch job:", "Next attempt:"):
        assert phrase in catalog
    assert "node.nodeType === Node.TEXT_NODE" in catalog


def test_aws_connection_interface_never_places_secret_content_in_javascript():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "/api/aws-secrets/refresh" in page
    assert "/api/aws-connections" in page
    assert "bootstrap_secret_access_key" not in page.split("<script>", 1)[1]


def test_aws_connection_template_is_formatted_and_copyable():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="aws-secret-json-template"' in page
    assert "copyAwsSecretTemplate()" in page
    assert "navigator.clipboard.writeText" in page
    assert ".json-key" in page and ".json-string" in page
    assert "Nenhuma conexão AWS cadastrada" in page


def test_registered_aws_connection_secrets_are_disabled_in_the_registration_form():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "registeredBySecret" in page
    assert "Secret já cadastrado na conexão" in page
    assert "id=\"aws-connection-register\"" in page
    assert "updateAwsConnectionRegistrationState" in page


def test_source_summary_identifies_its_aws_connection_with_an_orange_tag():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "aws-connection-tag" in page
    assert "aws_connection_label" in page
    assert "AWS Connections:" in page
    assert "#fb923c" in page


def test_transfer_queue_uses_compact_style_for_batch_job_id():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert ".queue-restore code" in page
    assert "padding:.18rem .35rem" in page
