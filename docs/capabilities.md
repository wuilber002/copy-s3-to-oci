# Capacidades e operação do RAIJIN

Este é o inventário vivo de produto do RAIJIN. Toda entrega que acrescente,
remova ou altere um fluxo operacional deve atualizar este documento no mesmo
commit. O objetivo é dar visibilidade rápida sobre **o que a plataforma é capaz
de fazer** e **como ela organiza a operação**.

## O que o RAIJIN faz

### Fontes, conexões e inventário

- Cadastra múltiplas conexões AWS, inclusive para contas diferentes, usando
  Secrets OCI com JSON versionado e um rótulo local imutável.
- Vincula várias fontes S3 a uma conexão AWS; cada conexão usa seu bucket de
  controle e prefixos gerados pelo RAIJIN para não misturar manifestos.
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
- Cria waves dinâmicas com previsão por objeto, limite de quantidade de objetos,
  dados históricos de transferência e horizonte de restore antecipado.
- Submete restores arquivados por S3 Batch Operations, com manifesto e relatório
  de conclusão persistidos no bucket de controle.
- Registra tentativa, aceite AWS, Batch job, polling, disponibilidade parcial e
  disponibilidade completa do restore.
- Permite duas estratégias: iniciar transferência somente após toda a wave estar
  restaurada ou liberar objetos à medida que se tornam disponíveis.
- Opera uma wave de transferência por vez, com múltiplos workers para copiar os
  objetos dessa wave em paralelo e limite de throughput configurável.
- Copia S3 para OCI preservando chave, tamanho, metadados compatíveis e tags
  registradas localmente.
- Retoma uploads multipart interrompidos usando checkpoints persistidos de
  `upload_id` e partes já aceitas pelo OCI.

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
- Executa em uma única VM OCI com PostgreSQL como banco e fila durável.
- Mantém backup lógico local do PostgreSQL, estado da VM e procedimentos de
  recuperação documentados.
- Mantém a interface somente em `localhost`; o acesso é feito por túnel SSH com
  chave pública/privada.
- Usa Instance Principal OCI e Secrets OCI; credenciais AWS não são exibidas ao
  navegador nem persistidas no banco.

## Como a operação funciona

### Telas da console

| Área | Finalidade |
| --- | --- |
| **Status** | Saúde global: serviços da VM, prontidão de credenciais, capacidade local, alertas e indicadores operacionais. |
| **Queue** | Console de acompanhamento: atividade de migração, fila de transferência, fila de discovery, auditorias profundas e inventário de bordo do pipeline dinâmico. |
| **Migrations** | Cadastro e seleção de sources, discovery, inventário, re-discovery, validação OCI, criação e administração de waves. |
| **Configurações** | Parâmetros operacionais, estratégia de transferência, conexões AWS, Secrets, buckets OCI, tarifas e pré-checks. |

Na interface web, os controles que iniciam ou alteram processos de maior
impacto exibem o ícone circular `i` dentro do próprio controle. O balão de
ajuda descreve finalidade, impacto operacional e condição de uso, sem exigir
que o operador saia da console. Exemplos: discovery,
reconciliação OCI, restores dinâmicos, fila completa, inventário de bordo,
recuperação de leases, Secrets, pré-check e tarifas públicas.

### Fluxo padrão por source

1. Cadastre uma conexão AWS baseada em um Secret OCI compatível.
2. Cadastre a source, selecionando a conexão e o bucket OCI de destino.
3. Execute discovery por API ou importe o S3 Inventory.
4. Revise inventário, estimativas e crie waves manualmente ou pelo pipeline
   dinâmico.
5. Coloque as waves na fila. O worker de governança submete e acompanha restore;
   o worker de transferência copia os objetos liberados.
6. Acompanhe a execução em **Queue** e consulte relatórios, manifestos e eventos
   por wave.
7. Execute **Validate OCI destination** ao finalizar ou quando houver suspeita de
   divergência. Use auditoria profunda somente quando a evidência normal não for
   suficiente.

### Pipeline dinâmico e inventário de bordo

Quando o pipeline dinâmico está ativo, o RAIJIN prevê a duração de cópia de cada
objeto a partir de tamanho, limite de throughput e histórico real. Com essas
previsões, cria waves dentro de um alvo de tempo e quantidade de objetos e
solicita restores antecipadamente dentro do horizonte configurado.

O **inventário de bordo**, acessível em **Queue**, apresenta:

- linha do tempo colorida com períodos planejados e observados de fila, restore
  e transferência;
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
