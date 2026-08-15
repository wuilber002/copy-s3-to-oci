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
    'Tier padrão de': 'Default restore',
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
    'Idioma': 'Language', 'Português': 'Portuguese'
  };
  const entries = Object.entries(ptToEn).sort(([a], [b]) => b.length - a.length);
  const enToPt = entries.map(([pt, en]) => [en, pt]).sort(([a], [b]) => b.length - a.length);
  let language = localStorage.getItem(STORAGE_KEY) === 'en' ? 'en' : 'pt';
  let translating = false;

  const translate = (value, direction) => {
    if (!value || !value.trim()) return value;
    let result = value;
    for (const [from, to] of direction) result = result.split(from).join(to);
    return result;
  };
  const localizeElement = (element) => {
    const direction = language === 'en' ? entries : enToPt;
    const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT, {
      acceptNode: node => ['SCRIPT', 'STYLE'].includes(node.parentElement?.tagName) ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT
    });
    const nodes = []; while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(node => { node.nodeValue = translate(node.nodeValue, direction); });
    element.querySelectorAll?.('[placeholder],[title],[aria-label],[data-help]').forEach(node => {
      ['placeholder', 'title', 'aria-label', 'data-help'].forEach(attribute => {
        if (node.hasAttribute(attribute)) node.setAttribute(attribute, translate(node.getAttribute(attribute), direction));
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
  window.setLanguage = value => { language = value === 'en' ? 'en' : 'pt'; localStorage.setItem(STORAGE_KEY, language); applyLanguage(); };
  window.addEventListener('DOMContentLoaded', () => {
    applyLanguage();
    new MutationObserver(records => { if (!translating) records.forEach(record => record.addedNodes.forEach(node => { if (node.nodeType === Node.ELEMENT_NODE) localizeElement(node); })); }).observe(document.body, {childList: true, subtree: true});
  });
})();
