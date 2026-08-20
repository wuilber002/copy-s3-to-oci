# Arquitetura

Uma única VM Linux em OCI hospeda a aplicação, PostgreSQL, orquestrador, workers e interface web. A interface escuta exclusivamente em `127.0.0.1`; a operação remota é feita por túnel SSH.

O PostgreSQL é a fonte de verdade e a fila durável. As tarefas usam reserva transacional, lease e heartbeat. Após uma queda ou reboot, tarefas com lease expirado são retomadas de forma idempotente.

## Princípios

- Uma onda tem no máximo 10 TB.
- Ondas podem ser criadas uma a uma, automaticamente para toda a origem ou automaticamente para um prefixo S3. A seleção é determinística por chave S3; a prévia informa objetos, bytes, estimativa de ondas e objetos acima do tamanho alvo.
- Um objeto maior que o alvo não bloqueia o planejamento: ele recebe uma onda exclusiva sinalizada no histórico. A criação automática é limitada a 10.000 ondas por ação como proteção operacional.
- Uma onda só pode ser excluída antes de uma tarefa ser assumida; a exclusão devolve seus objetos a `DISCOVERED`. Depois de restore, polling, transferência ou verificação iniciados, os dados são preservados para auditoria.
- Apenas uma onda pode ter uma tarefa de transferência em execução por vez. Dentro dessa onda, `Workers de transferência` controla quantos objetos são copiados simultaneamente. O worker recarrega essa configuração e o limite agregado de throughput antes de cada lote; mudanças na interface entram em vigor no próximo lote, sem reiniciar a VM. Arquivos já em cópia não são interrompidos.
- A origem é lida uma única vez por objeto e enviada em streaming para OCI Object Storage. `GetObject` fornece simultaneamente conteúdo e metadados, evitando uma chamada `HeadObject` adicional por arquivo. A consulta de tags pode ser desabilitada em Configurações quando não for requisito de migração, eliminando também `GetObjectTagging`.
- SHA-256 é calculado durante a leitura do S3. Para objetos pequenos, o valor é enviado ao `PutObject` e validado pelo OCI antes da aceitação. Para objetos grandes, cada parte multipart recebe SHA-256 próprio e o OCI valida todas as partes antes do commit. O tamanho-base da parte é configurável na console (64 MiB por padrão) e é gravado no checkpoint do objeto; para respeitar o máximo OCI de 10.000 partes, a plataforma o aumenta automaticamente em objetos muito grandes. A evidência nativa de entrega fica no PostgreSQL sem reler o destino.
- A reconciliação OCI sob demanda lista o destino paginado e compara chaves e tamanhos com o discovery persistido. Nos itens equivalentes, faz `HeadObject` somente no OCI para comparar a proveniência imutável gravada na cópia: ETag e data de última modificação da versão S3. Uma divergência reabre apenas os objetos e ondas afetados; não relê payload nem chama AWS.
- A onda recebe o estado **CONCLUÍDO** somente quando todos os seus objetos foram transferidos e possuem evidência criptográfica de aceitação pelo OCI. Após uma auditoria profunda sem divergências, ela passa para **CONCLUÍDO (AUDITADO)** (`VERIFIED` internamente).
- `VERIFY_WAVE` é uma **auditoria profunda** excepcional: ela relê integralmente o OCI e compara o SHA-256 linear calculado na origem. A criação exige confirmação explícita e o progresso por objeto é persistido para acompanhamento e recuperação.
- O Status mostra taxa atual de 15 segundos, throughput concluído, e arquivos transferidos por minuto/hora, todos calculados a partir de uma janela de até cinco minutos. Para restores, mantém o total acumulado de objetos arquivados solicitados e disponíveis. Objetos STANDARD não entram nas métricas de restore.
- Durante o polling de restore, o worker persiste o instante em que cada objeto ficou disponível e a data de expiração da cópia temporária retornada pelo S3 (`Restore`/`RestoreStatus`). Esses dados alimentam a fila, o relatório e a própria lista de waves: após a conclusão, a coluna **Restore** mostra a duração entre a solicitação e a disponibilidade de todos os objetos; enquanto houver objetos pendentes, mostra `Restore em andamento`. Isso permite medir o tempo real de restore e a primeira expiração iminente sem chamadas AWS adicionais. A expiração representa somente a cópia temporária em S3 Standard; o objeto arquivado original permanece preservado.
- O modo **dinâmico por duração** cria waves ainda estáticas, mas combina três guardrails: volume máximo, quantidade máxima de objetos e duração prevista. A previsão usa P75 de objetos já transferidos da mesma source por faixa de tamanho (`até 1 MiB`, `até 16 MiB`, `até 256 MiB`, grande e multipart) quando há pelo menos cinco amostras; sem histórico, usa overhead por objeto, tamanho, workers, throughput configurado e partes multipart. Medições com falha ou duração ausente não entram no aprendizado.
- A programação antecipada é opt-in em dois níveis: a chave global **Pipeline dinâmico de restores** e a marcação no formulário de criação. Para uma única faixa de transferência, a primeira janela começa após a margem configurada; BULK é solicitado até 48 h mais essa margem antes da janela e STANDARD até 12 h mais a margem. As tarefas `SUBMIT_BATCH_RESTORE` ficam persistidas com `available_at` futuro, portanto sobrevivem a reinicialização da VM. O histórico refina criações futuras; não altera silenciosamente waves já criadas ou restores já agendados.
- A fila de transferência no Status lista a onda ativa, seus workers, os bytes e arquivos concluídos e o tempo acumulado de cópia; as ondas restantes aparecem em ordem de processamento.
- Auditorias profundas aparecem separadamente no Status, com progresso de releitura, taxa, estimativa restante e divergências.
- Uma onda só é considerada ativamente transferindo quando sua tarefa `TRANSFER_WAVE` está em `RUNNING`. Um objeto que permaneça em `TRANSFERRING` após falha, pausa ou retry não mantém indevidamente a onda no quadro de atividade.
- Ao concluir uma wave, a console exibe sua duração operacional de cópia: do primeiro arquivo iniciado ao último arquivo concluído, incluindo eventuais interrupções e retomadas.
- O bloco **Atividade de migração** é independente da saúde da VM. Sua atualização automática pode ser desativada ou configurada entre 5 segundos e 5 minutos; essa preferência fica persistida no PostgreSQL da plataforma.
- O mesmo bloco mostra CPU e memória da VM. A CPU é uma média host-wide entre as execuções do timer de estado (normalmente um minuto); a memória usa `MemAvailable` do Linux para calcular a parcela efetivamente ocupada.
- Uma origem recebe a marca **Concluído** quando discovery e transferência terminarem e todos os objetos tiverem evidência de entrega validada pelo OCI; `VERIFIED` identifica adicionalmente objetos que passaram por auditoria profunda.
- Se a validação explícita do destino OCI detectar objeto ausente ou tamanho divergente, os objetos afetados voltam para `WAVE_ASSIGNED`, suas ondas para `READY_FOR_RESTORE` e a origem deixa de ser concluída. Nenhum restore ou retransferência é iniciado automaticamente; o operador usa **Reprocessar** quando decidir corrigir a divergência.
- Discovery usa somente `ListObjectsV2` e campos retornados pelo S3; não faz restore nem leitura de conteúdo. Cada página concluída grava seu `ContinuationToken`, total de páginas e objetos inseridos antes de solicitar a próxima. A duração persistida é tempo de execução acumulado, sem somar espera na fila. Após uma interrupção do worker/VM, a source volta automaticamente para a fila e retoma do checkpoint confirmado; uma falha de AWS pode ser retomada pelo operador sem relistar as páginas concluídas.
- Chamadas AWS são minimizadas. Restore usa S3 Batch Operations por onda; polling acompanha o estado da Batch job e escolhe a menor estratégia: `HeadObject` para uma wave archive pequena frente ao inventário da source, ou listagem paginada com `RestoreStatus` quando a varredura é mais econômica.
- Cada wave e cada source possuem uma estimativa de custo sob demanda. Ela calcula jobs/tarefas Batch, manifests, requests de discovery/polling/leitura, retrieval por classe e tier, retenção temporária, saída AWS, operações multipart OCI e armazenamento mensal. A plataforma coleta as AWS Price Lists públicas regionais de Amazon S3 e AWS Data Transfer sem usar credenciais, por padrão a cada sete dias e também sob comando manual; a saída AWS→OCI usa apenas a entrada regional `AWS Outbound` → `External`. Para cada campo, uma tarifa contratual da conexão substitui a tarifa pública; campos OCI permanecem configuráveis por conexão. Uma tarifa ausente torna o componente e o total correspondente explicitamente não estimados, em vez de assumir preço zero.
- O manifesto CSV gerado para Batch Operations usa estritamente os campos AWS `Bucket`, `Key` e, quando necessário, `VersionId`; as chaves são URL-encoded antes de serem gravadas.
- Os clientes AWS têm timeout explícito de conexão (10 segundos), leitura (120 segundos) e até quatro tentativas padrão do SDK. Depois disso, a tarefa durável entra em retry e preserva o checkpoint multipart OCI; uma conexão de rede travada não deve ocupar indefinidamente o único worker real.
- A chave do S3 é preservada no OCI. Metadados e tags são preservados no destino quando compatíveis e sempre no manifesto imutável da migração.

## Dimensionamento inicial

Para o caso inicial de 600 TB, ondas de 10 TB e rota de 1,2 Gbps:

| Recurso | Recomendação |
| --- | --- |
| Shape | VM.Standard.E5.Flex, ou flex x86 equivalente disponível |
| CPU | 8 OCPUs em produção; 2 OCPUs na validação inicial de 220 MB |
| Memória | 32 GB em produção; 8 GB na validação inicial de 220 MB |
| Boot volume | 500 GB |
| Workers de transferência iniciais | 4 |

Em 1,2 Gbps, 10 TB levam cerca de 18,5 h no limite teórico; planeje 22–30 h. A retenção padrão de restore deve ser 4 dias.

O tamanho mínimo de laboratório é 2 OCPUs/8 GB. Ele suporta PostgreSQL, a interface e poucos workers para validar o fluxo, mas não é indicado para ondas de 10 TB nem para usar todo o link de 1,2 Gbps.

## Recuperação

PostgreSQL, backups lógicos locais, WAL, logs e releases residem no boot volume persistente. Uma policy automática de backup do boot volume deve ser associada à VM, com retenção mínima de 14 dias. Backup local acelera restauração, mas o backup do volume protege contra perda da VM ou do disco.

O `user_data` do cloud-init é intencionalmente ignorado em atualizações Terraform: ele só executa no primeiro boot. Uma atualização de release nunca pode substituir uma VM que contém o banco de controle.
