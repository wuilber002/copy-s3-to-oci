# Capacidades do [RAIJIN](https://en.wikipedia.org/wiki/Raijin) e do [FUJIN](https://en.wikipedia.org/wiki/F%C5%ABjin)

Este é o inventário vivo dos dois programas que compõem a plataforma. Toda
entrega que acrescente, remova ou altere um fluxo operacional deve atualizar
este documento no mesmo commit.

## [RAIJIN](https://en.wikipedia.org/wiki/Raijin) — plano de controle de migração

<img src="../images/raijin-oracle-about.png" alt="Logo do RAIJIN" width="280">

O RAIJIN é o programa que inventaria fontes, planeja waves, governa restores,
coordena workers, transfere objetos, registra evidências e apresenta a console
operacional. Ele opera com integrações reais ou, quando explicitamente iniciado
no modo isolado, delega as integrações simuladas ao FUJIN.

### O que o RAIJIN faz

### Fontes, conexões e inventário

- Cadastra múltiplas conexões AWS, inclusive para contas diferentes, usando
  Secrets OCI com JSON versionado e um rótulo local imutável.
- Vincula várias fontes S3 a uma conexão AWS; cada conexão usa seu bucket de
  controle e prefixos gerados pelo RAIJIN para não misturar manifestos.
- Permite vários prefixes independentes em uma única source, mas impede que
  fontes ativas do mesmo bucket tenham escopos iguais ou sobrepostos. A regra
  usa a semântica literal do S3 (`app/` também cobre `app/images/`) e evita
  discovery, restore e transferência duplicados. Sources arquivadas não
  bloqueiam um novo cadastro.
- Descobre objetos por API S3 paginada, com checkpoint, retomada, limitação de
  requisições por conexão e acompanhamento em fila durável.
- Importa CSV/GZIP ou `manifest.json` do S3 Inventory para evitar chamadas de
  API em inventários grandes.
- Executa re-discovery controlado, com justificativa obrigatória, sem apagar
  inventário ou evidências de migração existentes.
- Identifica objetos novos e alterações de tamanho, ETag, versão ou última
  modificação quando esses campos estiverem disponíveis no inventário.
- Preserva a origem do discovery: API remota, S3 Inventory ou arquivo enviado.

### Waves, restore e transferência

- Cria waves manualmente por volume, em massa por limite de tamanho ou por
  prefixo; uma wave nunca excede 10 TB.
- Cria waves dinâmicas de forma adaptativa: a retenção define a janela útil de
  cópia, a reserva operacional protege a expiração e cada próxima wave é
  empacotada com previsão por objeto e histórico real de transferência.
- Submete restores arquivados por S3 Batch Operations, com manifesto e relatório
  de conclusão persistidos no bucket de controle.
- Registra tentativa, aceite AWS, Batch job, polling, disponibilidade parcial e
  disponibilidade completa do restore.
- Importa a evidência individual do completion report e apresenta no relatório
  da wave o código HTTP/AWS, a quantidade afetada, chaves de exemplo, causa e
  ação recomendada. `RestoreAlreadyInProgress` é reconhecido como aceite
  equivalente, sem criar um restore duplicado.
- Cada wave define sua política de liberação: iniciar a cópia após todos os
  objetos restaurados ou liberar cada objeto assim que sua disponibilidade é
  observada. Waves dinâmicas usam obrigatoriamente a liberação imediata.
- Opera uma **fila contínua de transferência** por source: a wave mantém as
  fronteiras de restore, custo e auditoria, mas não monopoliza os Raiju. Um
  objeto disponível de outra wave pode ser copiado assim que se torna mais
  urgente ou necessário para manter a lane ocupada.
- O operador configura globalmente o estoque mínimo, alvo e máximo da lane
  (padrões de 3h, 6h e 24h), os lotes normais (100 objetos ou 1 GiB) e os
  micro-lotes críticos (20 objetos ou 256 MiB). O Raikou usa esses limites
  para antecipar restores sem abrir janelas temporárias em excesso.
- Uma wave pode permanecer em **RESTORING** enquanto seus objetos disponíveis
  são drenados. Não existe limiar percentual de liberação: o Raikou usa o
  estoque real de trabalho com reserva mínima de 3 h, alvo de 6 h e máximo de
  24 h para alimentar a lane sem antecipar cópias restauradas sem necessidade.
- O scheduler mantém inicialmente dois restores concorrentes e uma terceira
  wave futura replanejável; com histórico suficiente, pode ampliar restores
  até o teto global configurado de quatro. Um slot de restore é liberado assim
  que a wave fica 100% disponível, mesmo que seus objetos ainda estejam na lane.
- O Raikou prioriza objetos já legíveis por expiração, duração prevista,
  viabilidade dentro da janela, tamanho e proteção contra starvation. Lotes
  normais têm até 100 objetos ou 1 GiB; urgências críticas usam micro-lotes de
  até 20 objetos ou 256 MiB e fazem preempção cooperativa somente entre objetos
  ou partes multipart seguras. Cada lote de despacho, motivo, prioridade,
  capacidade Raiju e intervalo executado ficam persistidos para auditoria. A
  cada vaga Raiju liberada, a prioridade e a capacidade são recalculadas e a
  parcela livre do limite de throughput é distribuída sem interromper I/O em
  curso. Somente waves sem submissão AWS podem ser
  reempacotadas ou ter horários alterados; Batch Jobs aceitos permanecem como
  evidência e não são recriados automaticamente.
- A transferência interna inicia com cinco **Raijus**, aumenta ou reduz a
  concorrência de cópia conforme a banda medida
  e nunca fica abaixo desse piso quando houver objetos suficientes.
- O **Raikou** é o worker de governança: executa discovery, planejamento, S3
  Batch Operations, polling de restore, reconciliação e auditorias. O **Raiju**
  é o worker operacional de transferência e retomada multipart.
- Copia S3 para OCI preservando chave, tamanho, metadados compatíveis e tags
  registradas localmente.
- Retoma uploads multipart interrompidos usando checkpoints persistidos de
  `upload_id` e partes já aceitas pelo OCI.
- No modo de simulação, o relógio virtual é **orientado a fases**: ele avança
  entre eventos planejados, mas fica congelado durante polling, streaming,
  multipart e retries do worker real. Isso impede expiração artificial da
  retenção enquanto o host executa trabalho local.
- O inventário de bordo registra, para cada wave simulada, os marcos virtuais
  de submissão, primeira e última disponibilidade, início e término da cópia;
  a linha do tempo não mistura estes marcos com o relógio real da VM.

### Integridade, reconciliação e reprocessamento

- Calcula SHA-256 durante a leitura da origem e valida a aceitação criptográfica
  pelo OCI durante o envio, sem uma segunda leitura integral do destino.
- Executa reconciliação sob demanda do destino OCI por chave, tamanho e
  proveniência gravada nos metadados.
- Executa auditoria profunda opcional SHA-256, que relê o destino; exige aviso e
  confirmação explícita por ser lenta e custosa.
- Ao encontrar divergência no destino, reabre somente os objetos e waves
  afetados para reprocessamento.
- Ao encontrar arquivo modificado no S3 que já tenha histórico de migração,
  mantém a versão anterior como evidência, exige validação OCI posterior e cria
  uma nova revisão elegível para uma nova wave. A versão histórica não é
  sobrescrita.

### Custos, segurança e resiliência

- Estima custos por source e wave a partir de tarifas AWS públicas ou tarifas
  específicas da conexão, quando habilitado.
- Considera Batch Operations, restore, requests S3, saída AWS, cópia temporária
  restaurada e, opcionalmente, componentes OCI configurados.
- Após a transferência, registra separadamente o custo observado da cópia
  temporária restaurada: o intervalo entre disponibilidade e entrega (ou
  expiração/instante atual, quando ainda aberto) é convertido para GiB-mês sem
  substituir a estimativa de retenção originalmente solicitada.
- Executa em uma única VM OCI com PostgreSQL como banco e fila durável.
- Mantém backup lógico local do PostgreSQL, estado da VM e procedimentos de
  recuperação documentados.
- Mantém a interface somente em `localhost`; o acesso é feito por túnel SSH com
  chave pública/privada.
- Usa Instance Principal OCI e Secrets OCI; credenciais AWS não são exibidas ao
  navegador nem persistidas no banco.

### Como a operação funciona

### Telas da console

| Área | Finalidade |
| --- | --- |
| **Status** | Saúde global: serviços da VM, prontidão de credenciais, capacidade local, alertas e indicadores operacionais. |
| **Queue** | Console de acompanhamento: atividade de migração, fila de transferência, fila de discovery, auditorias profundas e inventário de bordo do pipeline dinâmico. |
| **Migrations** | Cadastro e seleção de sources, discovery, inventário, re-discovery, validação OCI, criação e administração de waves. |
| **Configurações** | Parâmetros operacionais, estratégia de transferência, conexões AWS, Secrets, buckets OCI, tarifas e pré-checks. |

Alertas operacionais aparecem no topo para chamar a atenção imediata e podem
ser dispensados com `×` sem bloquear a console. A mesma condição permanece na
fila rotativa do rodapé enquanto estiver verdadeira. Um alerta dispensado só
volta ao topo se a condição for resolvida e ocorrer novamente.
O contador global de falhas considera somente a tarefa mais recente de cada
wave pertencente a uma source ativa. Falhas de sources arquivadas permanecem
no histórico auditável, mas deixam de representar uma pendência operacional
atual.

Na interface web, os controles que iniciam ou alteram processos de maior
impacto exibem o ícone circular `i` dentro do próprio controle. O balão de
ajuda descreve finalidade, impacto operacional e condição de uso, sem exigir
que o operador saia da console. Exemplos: discovery,
reconciliação OCI, restores dinâmicos, fila completa, inventário de bordo,
recuperação de leases, Secrets, pré-check e tarifas públicas.

### Fluxo padrão por source

1. Cadastre uma conexão AWS baseada em um Secret OCI compatível.
2. Cadastre a source, selecionando a conexão e o bucket OCI de destino.
   Prefixes pertencentes à mesma source podem ser informados em conjunto; não
   crie sources ativas que se cruzem no mesmo bucket.
3. Execute discovery por API ou importe o S3 Inventory.
4. Revise inventário, estimativas e crie waves manualmente ou pelo pipeline
   dinâmico.
5. Coloque as waves na fila. O Raikou submete e acompanha restores; os Raijus
   copiam os objetos liberados.
6. Acompanhe a execução em **Queue** e consulte relatórios, manifestos e eventos
   por wave.
7. Execute **Validate OCI destination** ao finalizar ou quando houver suspeita de
   divergência. Use auditoria profunda somente quando a evidência normal não for
   suficiente.

### Pipeline dinâmico e inventário de bordo

Quando o pipeline dinâmico está ativo, o RAIJIN prevê a duração de cópia de cada
objeto a partir de tamanho, limite de throughput e histórico real. A retenção
define a janela operacional, descontada uma reserva para variações e retries.
O Raijin materializa somente o horizonte inicial configurado (três waves por
padrão); conforme as waves observadas terminam, calcula e cria a próxima com
as medições mais recentes. O agendamento de restore é obrigatório nesse modo.

O **inventário de bordo**, acessível em **Queue**, apresenta:

- linha do tempo colorida com períodos planejados e observados de fila, restore,
  margem operacional e transferência; o hover de cada bloco mostra tipo,
  data/hora prevista ou iniciada e tempo decorrido/esperado;
- legenda dos estados;
- lista de waves com estado e datas de início e término;
- histórico persistente, consultável depois da conclusão da source.

### Re-discovery e arquivos alterados

O re-discovery é incremental e auditável:

- Arquivos novos entram diretamente como `DISCOVERED` e podem compor novas
  waves.
- Alterações em arquivos ainda sem wave atualizam o registro disponível.
- Alterações em arquivos já associados a uma wave são exibidas como `MODIFIED`.
  O operador deve validar o destino OCI após o re-discovery. Se a cópia
  histórica estiver íntegra, usa **Ver alterações → Preparar nova transferência**
  para criar a revisão nova, que fica em `DISCOVERED`.

## [FUJIN](https://en.wikipedia.org/wiki/F%C5%ABjin) — backend de simulação

<img src="../images/fujin-oracle-about.png" alt="Logo do FUJIN" width="280">

O FUJIN é um programa independente, com contrato de backend versionado e ciclo
de evolução próprio. Ele emula AWS S3, OCI Object Storage, rede e tempo para
que o RAIJIN execute o mesmo fluxo operacional sem acessar provedores reais.
O FUJIN não substitui os workers nem a lógica de governança do RAIJIN: substitui
somente os endpoints externos consumidos por eles.

### O que o FUJIN faz

O FUJIN é um gêmeo digital controlado da AWS, do OCI, da rede e do tempo. Seu
objetivo não é criar um fluxo alternativo que aprove waves artificialmente: os
mesmos *governance* e *transfer workers* usados em produção continuam
processando fila, *leases*, *polling*, restores, streaming, multipart, retries,
checkpoints, reconciliação e replanejamento.

Na criação de cenários, tamanhos lógicos e o orçamento físico do modo `DATA`
são informados em GB e convertidos para bytes somente ao montar o contrato
interno imutável. Quantidades, períodos e aceleração do relógio usam valores
inteiros com separação visual de milhar, evitando a edição operacional de
números extensos em bytes.

Já estão disponíveis: modos exclusivos `REAL`/`SIMULATION`, banco lógico
separado, handshake versionado, console dedicada, catálogo virtual, gerador de
bytes determinístico, restore e Batch Operations simulados com evidência por
objeto, relógio virtual, perfis de rede, transferência `CONTROL`, streaming
`DATA`, multipart retomável, SHA-256 independente no destino descartável,
auditoria profunda, orçamento físico, templates editáveis com snapshots
imutáveis por execução, falhas reproduzíveis,
housekeeping diário e reversível, purga física protegida por validação cruzada,
tombstone SHA-256, relatório agregado de evidências, clonagem fiel de
execuções, replanejamento pelas durações lógicas observadas e timeline visual
previsto/observado com decisões do scheduler. O catálogo `CONTROL` foi validado
com 100 TB lógicos e 640 mil objetos sem persistir payload.

### Modos e isolamento

- O RAIJIN operará em modo `REAL` ou `SIMULATION`, nunca nos dois ao mesmo
  tempo.
- O modo simulado usará o banco lógico `migration_simulation`, separado do
  banco `migration` usado pela operação real.
- Secrets, credenciais AWS e configuração OCI real não serão montadas no modo
  simulado; o backend real não poderá ser instanciado nesse modo.
- A troca de modo entrará primeiro em `DRAINING`, recusará novos trabalhos e
  somente continuará se não houver discovery, restore, transferência,
  multipart, polling ou auditoria em execução ou aguardando processamento.
- A interface exibirá um banner permanente e marcará sources, waves, eventos e
  relatórios como `SIMULATED`.
- Cada execução terá modalidade imutável `CONTROL` ou `DATA`. Para testar outro
  princípio, o operador clonará o cenário e iniciará uma execução independente.

### Inventário e buckets virtuais

- O operador poderá criar buckets, sources e objetos virtuais usando uma seed
  e uma distribuição de tamanhos, classes, prefixes, metadados e tags.
- Um cenário de 100 TB ou 1,5 PB será representado pelo catálogo lógico dos
  objetos; não será necessário criar o mesmo volume em disco.
- O PostgreSQL armazenará somente identidade, tamanho lógico, versão, storage
  class, datas, metadata, tags, seed de conteúdo, estado e evidências.
- `List` e `Head` responderão pelo catálogo persistido, permitindo que o
  discovery real do RAIJIN percorra o bucket virtual e seja avaliado em grande
  escala.
- A seed e a versão do cenário reproduzirão o mesmo inventário e a mesma
  sequência de eventos em execuções posteriores.

### Relógio virtual

- O FUJIN manterá horário real e horário virtual separados, sem alterar o
  relógio do Linux.
- Um fator configurável permitirá representar horas ou dias simulados em
  segundos reais.
- Batch jobs, disponibilidade de restore, polling, retenção, expiração e
  janelas planejadas usam o relógio virtual por uma abstração única de tempo.
- Leases, retries e disponibilidade da fila durável permanecem no relógio real;
  isso impede que uma data virtual futura bloqueie a reivindicação de tarefas.
- O banner exibe, em linhas alinhadas, o relógio virtual acelerado e o relógio
  real do sistema, incluindo o fator de aceleração e o estado pausado.
- Depois de reiniciar a VM, o cenário voltará pausado no último checkpoint;
  somente uma retomada explícita poderá avançar o relógio novamente.

### Batch Operations, restore e polling

- O backend AWS simulado receberá o mesmo manifesto lógico e responderá com
  job, estado e completion report equivalentes aos consumidos em produção.
- O cenário poderá aceitar, rejeitar ou aceitar parcialmente um job e poderá
  produzir falhas individuais reproduzíveis por objeto.
- A disponibilização poderá acontecer de uma vez, progressivamente, por grupos,
  fora do prazo ou nunca acontecer.
- Cada objeto manterá os horários simulados de solicitação, primeiro momento
  observado como disponível e expiração da cópia restaurada.
- O *governance worker* real executará o polling adaptativo. O FUJIN apenas
  responderá ao `Describe`, `List` ou `Head`; não alterará diretamente a wave,
  o objeto ou a task no banco do RAIJIN.
- Isso permitirá medir quantidade de consultas, atraso entre disponibilidade e
  detecção e comportamento das duas estratégias de liberação de arquivos.

### Rede e transferência lógica

- O perfil de rede definirá throughput, latência, jitter, throttling, períodos
  de indisponibilidade e recuperação ao longo do tempo virtual.
- A transferência lógica representará centenas de terabytes rapidamente,
  avançando progresso e tempo conforme o modelo de rede, sem movimentar o
  payload equivalente.
- Os mesmos handlers e estados de transferência serão usados; o adaptador
  simulado fornecerá progresso controlado em vez de acesso a uma nuvem real.
- Mudanças de banda deverão produzir métricas observadas e fazer o scheduler
  recalcular as próximas waves e janelas, preservando a decisão tomada no
  histórico operacional.
- A simulação lógica valida escala, governança e planejamento, mas não será
  apresentada como prova do caminho físico dos bytes.

### Simulação de dados

- O backend AWS simulado gerará bytes determinísticos sob demanda, inclusive
  por faixa (`offset` e `length`), sem arquivo físico e sem acesso à Internet.
- O transfer worker real fará streaming, calculará SHA-256 e executará
  multipart, checkpoints, retries e retomada exatamente pelo fluxo de
  produção.
- O backend OCI simulado calculará seus próprios checksums sobre os bytes
  efetivamente recebidos. Ele nunca aceitará como prova apenas o hash declarado
  pela origem ou pelo worker.
- Cada chunk recebido será consumido e descartado imediatamente. Nenhum payload
  será armazenado no PostgreSQL ou no filesystem.
- O banco persistirá somente o bucket virtual, evidências de recebimento,
  SHA-256 observados, partes multipart, manifesto ordenado, tamanho final,
  checkpoints, falhas e timestamps.
- Retomadas poderão requisitar novamente qualquer faixa do objeto porque a seed,
  a chave, a versão e o offset reproduzem exatamente os mesmos bytes usando
  memória limitada.
- O SHA-256 integral não será inferido a partir dos hashes das partes. A
  validação multipart comparará cada parte independentemente, sua ordem, seu
  tamanho, o manifesto completo e o SHA-256 integral calculado pelo worker.
- Uma auditoria profunda poderá regenerar deterministicamente o stream e
  recalcular o SHA-256 integral sem manter o objeto armazenado.
- Essa modalidade será executada com volumes físicos representativos menores;
  ela valida streaming e integridade, enquanto a transferência lógica valida
  escalas como 100 TB ou mais.
- O orçamento físico padrão será de 1 TB decimal, configurável antes da
  execução e sem limite máximo imposto pelo produto. A console apresentará a
  estimativa de impacto e pedirá confirmação para valores elevados.

### Destino OCI virtual e reconciliação

- O commit de um objeto só atualizará o catálogo OCI virtual depois que todas
  as partes esperadas forem recebidas e validadas.
- Objetos incompletos, divergentes, corrompidos ou com partes fora de ordem
  permanecerão identificados como falha e não serão promovidos silenciosamente.
- `List`, `Head`, validação do destino e reconciliação consultarão o estado do
  bucket virtual persistido.
- O cenário poderá representar objeto já existente, objeto ausente, tamanho ou
  metadata divergente, checksum inválido e indisponibilidade do bucket.
- A validação deverá reabrir somente os objetos afetados, permitindo provar o
  mesmo reprocessamento fino usado no ambiente real.

### Falhas, retomada e reprodutibilidade

- Falhas serão declaradas por cenário e poderão atingir objeto, parte, chamada,
  período de tempo ou quantidade de tentativas.
- Serão simulados timeout, throttling, desconexão, worker interrompido, parte
  multipart rejeitada, restore expirado e corrupção de dados.
- Toda falha aleatória usará seed e será persistida com o ponto exato de
  injeção, tornando o teste reproduzível.
- O FUJIN nunca consumirá a fila durável nem mudará diretamente os estados
  operacionais. A recuperação continuará sendo responsabilidade dos workers
  reais por leases, retries e checkpoints.
- Relatórios compararão previsão e execução, mostrarão falhas injetadas,
  decisões do scheduler e evidências de que nenhum endpoint real foi chamado.
- Templates poderão definir perfis conhecidos de restore, rede e falhas. Cada
  execução receberá uma cópia imutável do template, incluindo seed e regras,
  para que alterações futuras no modelo não modifiquem testes históricos.
- A reprodução criará uma nova execução a partir do snapshot persistido e
  repetirá a mesma sequência até que a evidência deixe de ser necessária.
- Restore, variação de rede e falhas probabilísticas usam a identidade estável
  do objeto, não UUIDs locais do banco. Assim, a mesma seed e o mesmo snapshot
  repetem tempos e pontos de falha inclusive em um catálogo clonado.
- A expiração da cópia temporária é simulada: `Head`, leitura e transferência
  deixam de considerar o objeto disponível, e ele pode receber uma nova
  solicitação de restore.

### Retenção e compatibilidade

- Cenários e evidências poderão ser excluídos manualmente ou pelo housekeeping,
  cuja retenção padrão será de 60 dias e poderá ser alterada na console.
- A retenção será contada depois que a execução atingir um estado terminal. O
  lifecycle será `ACTIVE → DEPRECATED → PURGE_ELIGIBLE → PURGED`: após 60 dias,
  os dados entram em depreciação reversível e permanecem em quarentena por mais
  30 dias antes de poderem ser removidos.
- Durante a quarentena, um administrador poderá restaurar o registro para
  `ACTIVE`. Antes da purga, uma nova verificação referencial será obrigatória;
  trabalhos ativos, templates e registros referenciados não serão apagados.
- A purga automática será configurável e poderá exigir aprovação manual. Um
  tombstone mínimo de auditoria será preservado, e os backups deverão cobrir o
  período completo de quarentena.
- RAIJIN e FUJIN serão atualizados juntos e aceitarão somente uma versão
  compatível do contrato interno, validada no startup.
- O gerador de dados terá versão persistida. Algoritmos antigos permanecerão
  disponíveis enquanto existirem cenários dependentes; depois disso, seguirão
  o mesmo ciclo de depreciação, e o código será retirado em uma release normal.
- A matriz reproduzível `scripts/validate-simulation-templates.py` executa os
  onze templates nativos com seeds registradas e exige evidência das falhas
  declaradas. Os validadores separados comprovam catálogo e planejamento de
  100 TB lógicos com aproximadamente 640 mil objetos.

### Experiência unificada no modo Simulation

- O modo `SIMULATION` preserva as telas operacionais normais: **Status**,
  **Queue**, **Migrations** e **Settings**. Sources virtuais, discoveries,
  waves, filas, relatórios e workers são acompanhados e operados pelos mesmos
  fluxos usados no modo real.
- Um banner vermelho permanente identifica o runtime isolado. O botão
  **Simulation**, exibido entre **Migrations** e a engrenagem de configurações,
  usa texto vermelho quando inativo e fundo vermelho quando selecionado, abrindo a console
  administrativa do backend virtual.
- A página **Simulation** concentra somente a criação imutável de cenários,
  templates, orçamento físico do modo `DATA`, relógio virtual, falhas injetadas,
  evidências, replay e housekeeping. Discovery, estratégia, waves e queue
  permanecem exclusivamente em **Migrations** e **Queue**.
- A página **Simulation** mantém o mesmo rodapé operacional das demais telas,
  sem ações contextuais próprias. Condições importantes percorrem o ticker
  enquanto verdadeiras e são removidas somente após sua resolução.
- No runtime simulado, endpoints de descoberta/configuração de AWS e OCI reais
  ficam indisponíveis por defesa em profundidade. A aplicação também não recebe
  credenciais de nuvem e usa o PostgreSQL isolado `migration_simulation`.
- O arquivo de saúde produzido pelo host é montado somente para leitura tanto
  no runtime real quanto no simulado; CPU, memória, timers e containers
  permanecem observáveis sem conceder controle do host à aplicação.
- A troca de modo continua mediada pelo host e é recusada enquanto houver
  tarefa ou discovery executável. Voltar ao modo `REAL` restaura o banco e as
  configurações reais sem misturar registros entre os ambientes.

## Capacidades em evolução

Estas capacidades já têm base implementada, mas devem continuar sendo refinadas
com testes de escala e operação real:

- Calibração do preditor de duração por tamanho, quantidade de objetos e dados
  históricos de cada conexão.
- Scheduler dinâmico de restores para reduzir a janela de cópia temporária sem
  deixar a transferência sem objetos disponíveis.
- Observabilidade de alto volume: métricas de API, latência, throttling,
  disponibilidade de restore e eficiência da previsão.
- Reconciliation em escala, priorizando validações por inventário e metadados em
  vez de leituras de payload.
- Cobertura de testes de carga, falhas de rede, reinício de VM e grandes objetos
  multipart.

## Referências relacionadas

- [Arquitetura](architecture.md)
- [Conexões AWS](aws-connections.md)
- [Estimativas de custo](cost-estimates.md)
- [Deployment e operação](deployment.md)
- [Recuperação](recovery-runbook.md)
- [Plano de validação](validation-test-plan.md)
