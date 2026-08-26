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
    assert 'id="aws-secret-template-modal"' in page
    assert "openAwsSecretTemplateModal()" in page
    assert "copyAwsSecretTemplate()" in page
    assert "navigator.clipboard.writeText" in page
    assert ".json-key" in page and ".json-string" in page
    assert "Nenhuma conexão AWS cadastrada" in page


def test_registered_aws_connection_secrets_are_disabled_in_the_registration_form():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "registeredBySecret" in page
    assert "Secret já cadastrado na conexão" in page
    assert "id=\"aws-connection-register\"" in page


def test_connection_api_limits_are_configurable_in_the_interface():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="aws-connection-limits-modal"' in page
    assert "editAwsConnectionLimits" in page
    assert "Limites API" in page
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


def test_source_transfer_strategy_and_report_operational_summary_are_visible():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="source-transfer-strategy"' not in page
    assert 'id="selected-source-transfer-strategy"' in page
    assert "/transfer-strategy" in page
    assert "Resumo operacional" in page
    assert "Média entre disponibilizações" in page


def test_aws_connection_sync_and_safe_configuration_controls_are_available():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "viewAwsConnectionConfiguration" in page
    assert "syncAwsConnection" in page
    assert "syncSourceAwsRegion" not in page
    assert "Sincronizar região AWS" not in page
    assert "Campos ocultos" in page
    assert "Tentativas de restore" in page


def test_aws_connection_configuration_opens_in_a_dismissible_modal():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="aws-connection-configuration-modal"' in page
    assert "closeAwsConnectionConfigurationModal()" in page
    assert "openModal('#aws-connection-configuration-modal')" in page
    handler = page[page.index("async function viewAwsConnectionConfiguration"):page.index("async function syncAwsConnection")]
    assert "modal-actions" not in handler
    assert "ACTIONABLE_FAILED" in page


def test_wave_report_is_a_scrollable_modal():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="wave-report-modal"' in page
    assert 'id="wave-report-content" class="report-content"' in page
    assert "openModal('#wave-report-modal')" in page
    assert ".report-content{max-height:72vh;overflow:auto" in page


def test_discovered_objects_and_source_cost_actions_live_in_the_discovery_summary():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    source_actions = page[page.index('<div class="row source-actions">'):page.index('<div id="source-summary"')]
    assert 'id="discovered-objects-modal"' in page
    assert 'id="inventory-page-size"' in page
    assert "openDiscoveredObjectsModal()" in page
    assert "const actions=$('#source-summary-actions')" in page
    assert "refreshAll()" not in source_actions
    assert ".source-actions{flex-wrap:nowrap;overflow:visible" in page
    assert ".source-actions .source-combobox{flex:0 0 490px" in page


def test_wave_report_distinguishes_aws_batch_evidence_from_raijin_polling():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "AWS submission:" in page
    assert "Batch execution:" in page
    assert "Completion evidence:" in page
    assert "Raijin polling:" in page


def test_object_detail_opens_in_a_scrollable_modal():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="object-detail-modal"' in page
    assert 'id="object-detail-content" class="report-content"' in page
    assert "closeObjectDetailModal()" in page
    handler = page[page.index("async function showObject"):page.index("async function loadWaves")]
    assert "openModal('#object-detail-modal')" in handler


def test_inline_code_uses_compact_chip_styling_instead_of_large_code_blocks():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "code,pre{" not in page
    assert "code{display:inline-block;max-width:100%;padding:.12rem .36rem" in page
    assert "pre code{display:block;max-width:none;padding:0" in page


def test_source_region_is_derived_and_read_only_from_the_aws_connection():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="region" required readonly' in page
    assert "applyAwsConnectionRegion(){const c=" in page
    assert "$('#region').value=c?c.default_region:''" in page


def test_source_summary_shows_discovery_duration_and_checkpoint_progress():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "discovery.duration_seconds" in page
    assert "discovery.pages_completed" in page
    assert "checkpoint salvo" in page


def test_discovery_is_selected_in_a_minimal_modal_with_remote_and_inventory_file_paths():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="discovery-modal"' in page
    assert 'onclick="openDiscoveryModal()" disabled>Discovery</button>' in page
    assert 'id="discovery-mode-remote"' in page
    assert 'id="discovery-mode-file"' in page
    assert 'id="inventory-file" type="file"' in page
    assert 'id="discovery-submit" type="submit">OK</button>' in page
    assert "new FormData()" in page
    assert "/inventory/upload" in page
    assert "mais de <b>1 milhão de objetos</b>" in page
    assert "input[type=checkbox],input[type=radio]{width:auto}" in page
    assert 'class="discovery-options"' in page
    assert "docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html" in page


def test_all_application_messages_use_raijin_notification_windows():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="notification-stack"' in page
    assert "function dismissNotification" in page
    assert "className=`notification ${bad?'error':'ok'}`" in page
    assert "stack.append(item)" in page


def test_all_confirmation_questions_use_the_raijin_modal_instead_of_browser_confirm():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="confirm-modal"' in page
    assert "function ask(message" in page
    assert "function finishConfirmation" in page
    assert "window.confirm" not in page
    assert "confirm(" not in page
    assert page.count("await ask(") >= 14


def test_laboratory_mode_and_discovery_origin_are_visible_and_explicit():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="set-laboratory-mode"' in page
    assert 'id="laboratory-mode-banner"' in page
    assert "syncLaboratoryModeBanner" in page
    assert "Discovery: ${escape(discoveryModeLabel(discovery.mode))}" in page


def test_source_prefixes_use_a_controlled_add_remove_list():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="prefix-input"' in page
    assert 'id="prefix-list"' in page
    assert "function addSourcePrefix" in page
    assert "function removeSourcePrefix" in page
    assert "sourcePrefixScopesOverlap" in page
    assert "max-height:7.4rem" in page


def test_source_form_opens_in_a_modal_with_a_fixed_prefix_workspace():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="create-source-action"' in page
    assert 'onclick="openNewSourceModal()">Criar nova origem</button>' in page
    assert 'id="source-modal" class="modal hidden"' in page
    assert 'id="source-form" class="source-form-layout"' in page
    assert "function openNewSourceModal()" in page
    assert "openModal('#source-modal')" in page
    assert "function closeSourceModal()" in page
    assert "#source-modal .prefix-list{align-content:start;height:10rem;max-height:10rem;overflow-y:auto" in page
    assert "closeSourceModal();await loadSources()" in page


def test_source_selector_uses_name_only_and_context_tags_are_in_the_heading():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="source-context-tags"' in page
    assert 'class="source-inventory-heading"' in page
    assert 'sources.map(x=>`<option value="${x.id}">${escape(x.name)}</option>`)' in page
    assert "<b>Prefixos S3</b>" in page


def test_wave_cost_estimate_and_connection_pricing_are_available_in_modals():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="cost-pricing-modal"' in page
    assert 'id="wave-cost-modal"' in page
    assert "editCostPricing" in page
    assert "showWaveCost" in page
    assert "Estimativa de custo" in page


def test_cost_estimation_has_a_global_operational_toggle():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="set-cost-estimation"' in page
    assert "costEstimationEnabled" in page
    assert "estimativa de custo está desabilitada" in page
    assert "wave-cost-action" in page
    assert "source-cost-action" in page
    assert page.count("button.textContent='💰 Estimativa'") == 2
    assert "button.textContent='💲'" not in page


def test_running_tasks_are_not_rendered_as_alerts_but_stale_leases_are():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "tarefa(s) em execução" not in page
    assert "tarefa(s) com lease expirado" in page
    assert "api('/api/observability')" in page


def test_cost_estimation_supports_public_aws_prices_and_per_connection_overrides():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="global-pricing-settings"' in page
    assert 'id="set-cost-pricing-auto-refresh"' in page
    assert 'id="set-cost-pricing-refresh-days"' in page
    assert "refreshGlobalAwsPricing" in page
    assert "lista pública AWS" in page
    assert 'id="cost-include-aws-transfer-out"' in page


def test_public_aws_pricing_controls_are_visible_when_cost_estimation_is_enabled():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "classList.toggle('hidden',!costEstimationEnabled)" in page
    assert "if(costEstimationEnabled)await loadGlobalAwsPricing()" in page


def test_cost_pricing_can_show_collected_public_rates_and_modals_lock_background_scroll():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="public-pricing-modal"' in page
    assert "showCollectedPublicPricing" in page
    assert "Ver valores coletados" in page
    assert 'id="global-pricing-settings"' in page
    assert 'id="cost-include-oci-costs"' in page
    assert "body.modal-open{overflow:hidden}" in page
    assert "syncModalScrollLock" in page


def test_collected_public_pricing_handler_keeps_modal_open_inside_its_try_block():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    start = page.index("async function showCollectedPublicPricing")
    handler = page[start:page.index("\nasync function saveCostPricing", start)]
    assert "openModal('#public-pricing-modal')}catch" in handler
    assert "renderCollectedPublicPricing(select.value)" in handler
    assert 'id="public-pricing-region"' in page
    assert "unitRateMoney" in page


def test_cost_symbols_show_hover_summary_and_keep_clickable_detail():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "loadWaveCostTooltip" in page
    assert "loadSourceCostTooltip" in page
    assert "costTooltip(data)" in page
    assert ".cost-action[data-cost-help]:hover::after" in page
    assert "Custo único ${complete?'estimado':'parcial'}" in page


def test_wave_creation_uses_one_shared_action_for_every_method():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert 'id="wave-create-submit"' in page
    assert 'onclick="submitSelectedWaveMethod()" disabled>Criar onda</button>' in page
    assert 'id="wave-creation-empty"' in page
    assert "function syncSourceDependentControls" in page
    assert "queueAll.disabled=true" in page
    assert "function submitSelectedWaveMethod()" in page
    assert "manual:'#wave-form'" in page
    assert "automatic:'#automatic-wave-form'" in page
    assert "prefix:'#prefix-wave-form'" in page
    assert "dynamic:'#dynamic-wave-form'" in page
    assert "<h3>Onda manual</h3>" not in page
    assert "<h3>Ondas por prefixo S3</h3>" not in page
    assert "<h3>Criação dinâmica</h3>" not in page


def test_buttons_use_content_width_and_standard_horizontal_spacing():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "width:max-content!important" in page
    assert "padding-left:1em!important" in page
    assert "padding-right:1em!important" in page
    assert "button.hidden{display:none!important}" in page
    assert 'id="waves-queue-all" class="secondary hidden" onclick="queueAllPlannedWaves()" disabled' in page
    assert "queueAll.classList.toggle('hidden',!hasSource)" in page
    assert 'id="source-validate-destination"' in page
    assert 'id="source-edit-action"' in page


def test_operational_history_emphasizes_selected_source_in_title():
    page = Path("app/static/index.html").read_text()

    assert 'id="operational-history-source"' in page
    assert "function syncOperationalHistorySource()" in page
    assert "'All sources':'Todos os sources'" in page
    assert "`— ${source?.name||allSources}`" in page
    assert "syncOperationalHistorySource();openModal('#operational-history-modal')" in page


def test_migration_tools_are_contextual_footer_modals_with_critical_ticker():
    page = Path("app/static/index.html").read_text()
    assert 'id="operational-statusbar" class="operational-statusbar"' in page
    assert 'id="statusbar-migrations-actions" class="statusbar-actions hidden" data-footer-view="migrations"' in page
    assert 'id="durable-queue-modal" class="modal hidden"' in page
    assert 'id="operational-history-modal" class="modal hidden"' in page
    assert '<section class="card"><h2>Fila durável</h2>' not in page
    assert '<section class="card"><h2>Histórico operacional</h2>' not in page
    assert "function syncStatusbarContext(view)" in page
    assert "function updateOperationalMessages(operations,platform,observability)" in page
    assert "setInterval(refreshOperationalMessages,30000)" in page
    assert "operationalMessages.length" in page


def test_wave_report_explains_restore_failures_and_supports_evidence_recovery():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "function renderWaveReportOverall" in page
    assert "Status geral:" in page
    assert "Polling de disponibilidade" in page
    assert "wave-report-overall" in page
    assert "function renderRestoreDiagnosis" in page
    assert "Processamento do restore" in page
    assert "restoreDiagnosisMessage" in page
    assert "Continue o polling de disponibilidade." in page
    assert "Código AWS" in page
    assert "Ação recomendada" in page
    assert "retry-restore-evidence" in page
    assert "Esta ação não cria nem submete um novo restore" in page


def test_source_modal_keeps_original_full_width_form_layout():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert "#source-modal .modal-panel{width:min(1400px,100%)}" in page
    assert 'id="source-form" class="source-form-layout"' in page


def test_simulation_mode_keeps_the_regular_console_and_exposes_a_red_admin_page():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    simulation = (ROOT / "app/static/simulation.html").read_text(encoding="utf-8")
    main = (ROOT / "app/main.py").read_text(encoding="utf-8")

    assert 'id="simulation-nav" class="simulation-nav hidden"' in page
    assert 'id="simulation-runtime-banner"' in page
    assert "location.href='/simulation'" in page
    assert "with open(\"app/static/index.html\"" in main
    assert '@app.get("/simulation", response_class=HTMLResponse)' in main
    assert 'class="simulation-active" href="/simulation">Simulation</a>' in simulation
    assert 'href="/?view=dashboard">Status</a>' in simulation
    assert 'href="/?view=queue">Queue</a>' in simulation
    assert 'href="/?view=migrations">Migrations</a>' in simulation
    assert page.index('id="simulation-nav"') < page.index('class="gear secondary"')
    assert simulation.index('class="simulation-active"') < simulation.index('class="gear"')
    assert ".simulation-nav{background:transparent!important" in page
    assert ".simulation-active{background:#b91c1c" in simulation
    assert "body.simulation-mode #alerts{top:134px}" in page
    assert "body.simulation-mode .notification-stack{top:138px}" in page
    assert "Open in Migrations" in simulation
    assert '>Discovery</button><button onclick="createWaves' not in simulation
    assert '>Create dynamic waves</button>' not in simulation
    assert '>Queue all</button>' not in simulation


def test_operational_top_alerts_are_dismissible_without_removing_the_footer_condition():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")

    assert "const dismissedTopAlerts=new Set()" in page
    assert "function dismissTopAlert(key)" in page
    assert "function renderTopAlerts(alerts)" in page
    assert 'class="alert-dismiss"' in page
    assert "if(!activeKeys.has(key))dismissedTopAlerts.delete(key)" in page
    assert "updateOperationalMessages(d,p,o)" in page
    assert "renderTopAlerts(alerts)" in page


def test_simulation_admin_page_keeps_the_persistent_operational_footer():
    simulation = (ROOT / "app/static/simulation.html").read_text(encoding="utf-8")

    assert 'id="operational-statusbar" class="operational-statusbar"' in simulation
    assert 'id="statusbar-ticker" class="statusbar-ticker idle"' in simulation
    assert "function updateOperationalMessages(operations,platform,observability)" in simulation
    assert "async function refreshOperationalMessages()" in simulation
    assert "setInterval(refreshOperationalMessages,30000)" in simulation


def test_simulation_scenario_form_uses_grouped_integers_and_gb_units():
    simulation = (ROOT / "app/static/simulation.html").read_text(encoding="utf-8")

    assert "Logical size (GB)" in simulation
    assert "DATA physical budget (GB)" in simulation
    assert 'value="100.000"' in simulation
    assert 'value="1.000"' in simulation
    assert 'value="3.600"' in simulation
    assert "const GB_BYTES=1_000_000_000" in simulation
    assert "logical_size_bytes:logicalSizeGb*GB_BYTES" in simulation
    assert "physical_budget_bytes:physicalBudgetGb*GB_BYTES" in simulation
    assert "function prepareIntegerFields()" in simulation
    assert "stepIntegerInput(input,direction)" in simulation
    assert 'step="0.01"' not in simulation


def test_public_project_links_use_current_repository_name():
    repository_url = "github.com/wuilber002/raijin-data-migration"
    assert repository_url in (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    assert repository_url in (ROOT / "README.md").read_text(encoding="utf-8")
    assert repository_url in (ROOT / "terraform/orm/schema.yaml").read_text(encoding="utf-8")
    assert repository_url in (ROOT / "terraform/orm/variables.tf").read_text(encoding="utf-8")


def test_flight_board_modal_has_only_one_vertical_scroll_container():
    page = (ROOT / "app/static/index.html").read_text(encoding="utf-8")

    assert "#flight-board-modal .modal-panel{display:flex;flex-direction:column" in page
    assert "max-height:calc(100vh - 2rem);overflow:hidden" in page
    assert "#flight-board-content{min-height:0;max-height:none;overflow-y:auto;overflow-x:hidden}" in page
