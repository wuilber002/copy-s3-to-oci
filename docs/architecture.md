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
- SHA-256 é calculado durante o streaming e persistido como evidência de origem. A cópia não relê imediatamente o objeto no OCI; a comparação SHA-256 e a transição para `VERIFIED` ocorrem na tarefa `VERIFY_WAVE`, enfileirada explicitamente pelo operador. Isso separa transferência e auditoria de conteúdo, reduzindo chamadas e tráfego durante a cópia.
- O Painel mostra taxa atual de 15 segundos, throughput concluído, e arquivos transferidos por minuto/hora, todos calculados a partir de uma janela de até cinco minutos. Para restores, mantém o total acumulado de objetos arquivados solicitados e disponíveis, além do ritmo de disponibilidade em arquivos/minuto e arquivos/hora. Objetos STANDARD não entram nas métricas de restore.
- Enquanto uma onda estiver transferindo, o Painel também lista fonte e onda com arquivos e bytes transferidos em relação ao total. Objetos em `TRANSFERRED` e `VERIFIED` contam como transferidos; a onda deixa essa lista ao concluir a cópia ou ao ser interrompida.
- Uma onda só é considerada ativamente transferindo quando sua tarefa `TRANSFER_WAVE` está em `RUNNING`. Um objeto que permaneça em `TRANSFERRING` após falha, pausa ou retry não mantém indevidamente a onda no quadro de atividade.
- O bloco **Atividade de migração** é independente da saúde da VM. Sua atualização automática pode ser desativada ou configurada entre 5 segundos e 5 minutos; essa preferência fica persistida no PostgreSQL da plataforma.
- O mesmo bloco mostra CPU e memória da VM. A CPU é uma média host-wide entre as execuções do timer de estado (normalmente um minuto); a memória usa `MemAvailable` do Linux para calcular a parcela efetivamente ocupada.
- Uma origem recebe a marca **Concluído** somente se o discovery estiver concluído, houver ao menos um objeto no inventário e todos os objetos estiverem em `VERIFIED`. Assim, uma origem totalmente copiada, mas ainda sem a verificação manual de integridade, não é encerrada indevidamente.
- Se a validação explícita do destino OCI detectar objeto ausente ou tamanho divergente, os objetos afetados voltam para `WAVE_ASSIGNED`, suas ondas para `READY_FOR_RESTORE` e a origem deixa de ser concluída. Nenhum restore ou retransferência é iniciado automaticamente; o operador usa **Reprocessar** quando decidir corrigir a divergência.
- Discovery usa somente listagem e campos retornados pelo S3; não faz restore nem leitura de conteúdo.
- Chamadas AWS são minimizadas. Restore usa S3 Batch Operations por onda; polling usa estado da Batch job e listagem paginada com `RestoreStatus`.
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
