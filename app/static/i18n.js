(() => {
  const STORAGE_KEY = 'raijin-ui-language';
  const ptToEn = {
    'Console operacional local. A tela não chama AWS nem OCI diretamente; workers controlados executam as integrações posteriormente.': 'Local operations console. This screen does not call AWS or OCI directly; controlled workers perform integrations afterwards.',
    'Saúde da plataforma': 'Platform health',
    'Carregando…': 'Loading…',
    'Carregando transferências ativas…': 'Loading active transfers…',
    'Carregando estado dos serviços…': 'Loading service status…',
    'Atividade de migração': 'Migration activity',
    'Atualizar agora': 'Refresh now',
    'Atualização automática': 'Auto refresh',
    'Intervalo': 'Interval',
    '5 segundos': '5 seconds', '10 segundos': '10 seconds', '15 segundos': '15 seconds', '30 segundos': '30 seconds', '1 minuto': '1 minute', '5 minutos': '5 minutes',
    'Fila de transferência': 'Transfer queue',
    'Carregando fila de transferência…': 'Loading transfer queue…',
    'Auditorias profundas': 'Deep audits',
    'Nenhuma auditoria profunda em execução.': 'No deep audit is running.',
    'Credenciais e integrações': 'Credentials and integrations',
    'Validar agora': 'Validate now',
    'Ainda não validado nesta sessão.': 'Not validated in this session yet.',
    'Fila e atividades recentes': 'Queue and recent activity',
    'Abrir Migrações': 'Open Migrations',
    'Plataforma de migração de dados': 'Data migration platform',
    'Versão': 'Version',
    'Princípios da plataforma': 'Platform principles',
    'Controle': 'Control', 'Ondas persistentes': 'Persistent waves',
    'Planejamento, fila, retomada e histórico sobrevivem a reinicializações da VM.': 'Planning, queueing, resuming, and history survive VM restarts.',
    'Eficiência': 'Efficiency', 'Chamadas minimizadas': 'Minimized calls',
    'Discovery e restores são orquestrados para reduzir operações cobradas na AWS.': 'Discovery and restores are orchestrated to reduce billable AWS operations.',
    'Confiança': 'Trust', 'Integridade comprovada': 'Proven integrity',
    'A cópia registra evidências SHA-256; auditoria profunda fica disponível sob demanda.': 'Transfers record SHA-256 evidence; deep audit is available on demand.',
    'Configuração operacional': 'Operational configuration',
    'Workers de': 'Workers', 'transferência': 'transfer',
    'Limite de throughput': 'Throughput limit', 'Tamanho padrão da': 'Default wave', 'onda (TB)': 'size (TB)',
    'Retenção padrão': 'Default retention', 'do restore (dias)': 'for restore (days)',
    'Tier padrão de': 'Default',
    'Lease da tarefa': 'Task lease', '(segundos)': '(seconds)',
    'Worker de': 'Simulation', 'simulação': 'worker', 'Habilitar': 'Enable',
    'Salvar parâmetros': 'Save operational', 'operacionais': 'parameters', 'Salvar configuração': 'Save configuration',
    'Configuração AWS': 'AWS configuration',
    'ARN da role AWS de migração': 'AWS migration role ARN',
    'ARN da role AWS Batch Operations': 'AWS Batch Operations role ARN',
    'Bucket AWS de controle': 'AWS control bucket', 'Prefixo no bucket de controle': 'Control bucket prefix',
    'Preservar tags S3': 'Preserve S3 tags', 'Worker AWS/OCI real': 'Real AWS/OCI worker',
    'Inventário de buckets OCI': 'OCI bucket inventory', 'Atualizar buckets OCI': 'Refresh OCI buckets',
    'Prontidão das integrações': 'Integration readiness', 'Executar pré-check': 'Run pre-check',
    'Pré-check ainda não executado.': 'Pre-check has not run yet.',
    'Nova origem': 'New source', 'Nome da origem': 'Source name', 'Bucket S3': 'S3 bucket', 'Prefixo S3 (opcional)': 'S3 prefix (optional)',
    'Região AWS': 'AWS region', 'Bucket OCI de destino': 'Destination OCI bucket', 'Cadastrar origem': 'Create source',
    'Fontes e inventário': 'Sources and inventory', 'Executar discovery': 'Run discovery', 'Validar destino OCI': 'Validate OCI destination',
    'Editar origem': 'Edit source', 'Excluir': 'Delete', 'Arquivar': 'Archive', 'Atualizar': 'Refresh',
    'Cadastre ou selecione uma origem.': 'Create or select a source.', 'Objetos descobertos': 'Discovered objects',
    'Exibir por página': 'Show per page', 'Buscar chave S3': 'Search S3 key', 'Buscar': 'Search',
    'Criar ondas': 'Create waves', 'Método de criação': 'Creation method',
    'Criar uma onda manualmente': 'Create one wave manually', 'Criar todas automaticamente': 'Create all automatically', 'Criar automaticamente por prefixo': 'Create automatically by prefix',
    'Onda manual': 'Manual wave', 'Nome da onda': 'Wave name', 'Quantidade de dados': 'Data amount', 'Unidade de medida': 'Unit of measure',
    'Retenção do restore (dias)': 'Restore retention (days)', 'Tier de restore': 'Restore tier', 'Criar onda manual': 'Create manual wave',
    'Ondas automáticas': 'Automatic waves', 'Tamanho alvo por onda': 'Target size per wave', 'Unidade': 'Unit', 'Criar todas as ondas': 'Create all waves',
    'Ondas por prefixo S3': 'Waves by S3 prefix', 'Prefixo de objetos': 'Object prefix', 'Criar ondas do prefixo': 'Create prefix waves',
    'Ondas': 'Waves', 'Colocar todas na fila': 'Queue all', 'Relatório da onda selecionada': 'Selected wave report',
    'Selecione “Relatório” em uma onda.': 'Select “Report” on a wave.', 'Detalhe do objeto': 'Object detail', 'Selecione uma chave no inventário.': 'Select a key in the inventory.',
    'Fila durável': 'Durable queue', 'Todos os estados': 'All states', 'Recuperar tarefas com lease expirado': 'Recover tasks with expired lease',
    'Exportar CSV': 'Export CSV', 'Histórico operacional': 'Operational history', 'Atualizar histórico': 'Refresh history', 'Exportar CSV completo': 'Export full CSV',
    'Auditoria profunda SHA-256': 'Deep SHA-256 audit', 'Cancelar': 'Cancel', 'Iniciar auditoria': 'Start audit',
    'Status': 'Status', 'Migrações': 'Migrations', 'Configurações': 'Settings', 'Sobre o RAIJIN': 'About RAIJIN',
    'Arquivos': 'Files', 'Dados': 'Data', 'Taxa': 'Rate', 'Tempo de cópia': 'Copy time', 'Divergências': 'Mismatches',
    'Fonte / onda': 'Source / wave', 'Fonte': 'Source', 'Onda': 'Wave', 'Objetos': 'Objects', 'Tamanho': 'Size', 'Ações': 'Actions',
    'Relatório': 'Report', 'Manifesto CSV': 'Manifest CSV', 'Pausar': 'Pause', 'Retomar': 'Resume', 'Reprocessar': 'Reprocess',
    'Verificar integridade': 'Verify integrity', 'Auditoria profunda': 'Deep audit',
    'Nenhuma onda criada.': 'No wave created.', 'Selecione uma origem.': 'Select a source.',
    'Nenhuma onda aguardando processamento.': 'No wave is waiting for processing.', 'Transferências ativas': 'Active transfers',
    'Nenhuma onda com transferência ativa.': 'No wave has an active transfer.', 'Ocioso': 'Idle', 'Em execução': 'Running', 'Aguardando': 'Waiting',
    'Arquivos transferidos': 'Transferred files', 'Transferência concluída': 'Completed transfer', 'Taxa atual': 'Current rate',
    'Memória da VM': 'VM memory', 'CPU da VM': 'VM CPU', 'Restores': 'Restores',
    'disponíveis': 'available', 'solicitados': 'requested', 'concluídos': 'completed', 'em cópia': 'copying',
    'Idioma': 'Language', 'Português': 'Portuguese',
    'Fontes': 'Sources', 'Inventário': 'Inventory', 'Fila pronta': 'Ready queue', 'Fila em execução': 'Running queue', 'Espaço livre': 'Free space',
    'Serviços da VM': 'VM services', 'Serviço da plataforma': 'Platform service', 'Aplicação web': 'Web application',
    'Timer de backup PostgreSQL': 'PostgreSQL backup timer', 'Timer de estado': 'Status timer', 'Worker de simulação': 'Simulation worker',
    'Nenhuma auditoria profunda aguardando ou em execução.': 'No deep audit is queued or running.',
    'Valida as Secrets OCI e os ARNs configurados localmente. Quando preenchidos, testa a credencial AWS e a role de migração sem listar ou restaurar objetos.': 'Validates locally configured OCI Secrets and ARNs. When supplied, it tests the AWS credential and migration role without listing or restoring objects.',
    'Selecione uma origem': 'Select a source', 'Nome completo ou trecho do caminho': 'Full name or path fragment',
    'Atualize e selecione um bucket OCI': 'Refresh and select an OCI bucket',
    'Ex.: arquivo-legado': 'Ex.: legacy-archive', 'Ex.: meu-bucket-origem': 'Ex.: my-source-bucket', 'Ex.: dados/2024/': 'Ex.: data/2024/',
    'Selecione a próxima sequência de objetos descobertos da origem até o limite. Ela gera uma tarefa persistente; o restore só será submetido pelo worker AWS após sua configuração.': 'Selects the next sequence of discovered source objects up to the limit. It creates a persistent task; restore is submitted by the AWS worker only after configuration.',
    'O worker simulado não acessa AWS ou OCI. Quando habilitado, ele avança ondas fictícias por restore, polling e transferência para testar fila, retomada e auditoria.': 'The simulation worker does not access AWS or OCI. When enabled, it advances fictional waves through restore, polling and transfer to test queueing, resumption and auditing.',
    'Último backup lógico:': 'Latest logical backup:', 'Nenhum backup lógico encontrado ainda.': 'No logical backup found yet.',
    'Estado gerado em': 'Status generated at', 'Estado dos serviços da VM indisponível.': 'VM service status unavailable.',
    'Aguardando primeira atualização': 'Waiting for first refresh', 'Estado dos serviços indisponível:': 'Service status unavailable:',
    'Nenhuma origem com transferência ativa.': 'No source has an active transfer.',
    'Não foi possível carregar atividade de migração.': 'Could not load migration activity.',
    'Os ARNs identificam as roles IAM utilizadas pela plataforma. Eles não são credenciais e ficam persistidos localmente no PostgreSQL da VM.': 'The ARNs identify the IAM roles used by the platform. They are not credentials and are stored locally in the VM PostgreSQL database.',
    'Consulta manualmente o OCI Resource Search no tenancy inteiro e guarda a lista localmente. A consulta não concede acesso a objetos; a policy da VM continua determinando quais buckets podem ser usados como destino.': 'Manually queries OCI Resource Search across the tenancy and stores the list locally. The query does not grant object access; the VM policy still determines which buckets can be used as destinations.',
    'Usa a identidade dinâmica da VM para OCI e, quando configurada, valida AWS por STS/AssumeRole. Não revela Secrets, lista objetos nem grava no bucket.': 'Uses the VM dynamic identity for OCI and, when configured, validates AWS through STS/AssumeRole. It does not reveal Secrets, list objects, or write to a bucket.',
    'RAIJIN conduz migrações controladas de AWS S3 para OCI Object Storage, com inventário persistente, ondas operacionais, retomada segura, rastreabilidade e verificação criptográfica de integridade.': 'RAIJIN performs controlled migrations from AWS S3 to OCI Object Storage with persistent inventory, operational waves, safe resumption, traceability, and cryptographic integrity verification.',
    'Inspirado em Raijin, a divindade japonesa do trovão, o emblema representa força, movimento e condução precisa: cada objeto sai da origem com evidências de transferência e chega ao destino preservando sua integridade operacional.': 'Inspired by Raijin, the Japanese deity of thunder, the emblem represents strength, movement, and precise guidance: every object leaves the source with transfer evidence and reaches the destination while preserving its operational integrity.',
    'Ver projeto no GitHub ↗': 'View project on GitHub ↗'
  };
  let language = localStorage.getItem(STORAGE_KEY) === 'en' ? 'en' : 'pt';
  let translating = false;
  const textSources = new WeakMap();
  const attributeSources = new WeakMap();

  const translate = value => {
    if (!value || !value.trim() || language !== 'en') return value;
    const prefix = value.match(/^\s*/)?.[0] || '';
    const suffix = value.match(/\s*$/)?.[0] || '';
    const core = value.slice(prefix.length, value.length - suffix.length);
    return `${prefix}${ptToEn[core] || core}${suffix}`;
  };
  const localizeElement = (element) => {
    const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT, {
      acceptNode: node => ['SCRIPT', 'STYLE'].includes(node.parentElement?.tagName) ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT
    });
    const nodes = []; while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(node => { if (!textSources.has(node)) textSources.set(node, node.nodeValue); node.nodeValue = translate(textSources.get(node)); });
    element.querySelectorAll?.('[placeholder],[title],[aria-label],[data-help]').forEach(node => {
      ['placeholder', 'title', 'aria-label', 'data-help'].forEach(attribute => {
        if (!node.hasAttribute(attribute)) return;
        if (!attributeSources.has(node)) attributeSources.set(node, {});
        const sources = attributeSources.get(node);
        if (!(attribute in sources)) sources[attribute] = node.getAttribute(attribute);
        node.setAttribute(attribute, translate(sources[attribute]));
      });
    });
  };
  const applyLanguage = () => {
    translating = true;
    document.documentElement.lang = language === 'en' ? 'en-US' : 'pt-BR';
    document.querySelectorAll('body > :not(script), .modal').forEach(localizeElement);
    const selector = document.querySelector('#language-selector'); if (selector) selector.value = language;
    translating = false;
  };
  window.raijinLocale = () => language === 'en' ? 'en-US' : 'pt-BR';
  window.uiText = text => language === 'en' ? (ptToEn[text] || text) : text;
  window.setLanguage = value => { language = value === 'en' ? 'en' : 'pt'; localStorage.setItem(STORAGE_KEY, language); applyLanguage(); document.dispatchEvent(new CustomEvent('raijin:language-changed')); };
  window.addEventListener('DOMContentLoaded', () => {
    applyLanguage();
    new MutationObserver(records => { if (!translating) records.forEach(record => record.addedNodes.forEach(node => { if (node.nodeType === Node.ELEMENT_NODE) localizeElement(node); })); }).observe(document.body, {childList: true, subtree: true});
  });
})();
