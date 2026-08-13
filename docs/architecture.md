# Arquitetura

Uma única VM Linux em OCI hospeda a aplicação, PostgreSQL, orquestrador, workers e interface web. A interface escuta exclusivamente em `127.0.0.1`; a operação remota é feita por túnel SSH.

O PostgreSQL é a fonte de verdade e a fila durável. As tarefas usam reserva transacional, lease e heartbeat. Após uma queda ou reboot, tarefas com lease expirado são retomadas de forma idempotente.

## Princípios

- Uma onda tem no máximo 10 TB.
- A origem é lida uma única vez por objeto e enviada em streaming para OCI Object Storage.
- SHA-256 é calculado durante o streaming e é a evidência principal de integridade; MD5 é registrado como evidência complementar.
- Discovery usa somente listagem e campos retornados pelo S3; não faz restore nem leitura de conteúdo.
- Chamadas AWS são minimizadas. Restore usa S3 Batch Operations por onda; polling usa estado da Batch job e listagem paginada com `RestoreStatus`.
- A chave do S3 é preservada no OCI. Metadados e tags são preservados no destino quando compatíveis e sempre no manifesto imutável da migração.

## Dimensionamento inicial

Para o caso inicial de 600 TB, ondas de 10 TB e rota de 1,2 Gbps:

| Recurso | Recomendação |
| --- | --- |
| Shape | VM.Standard.E5.Flex, ou flex x86 equivalente disponível |
| CPU | 8 OCPUs |
| Memória | 32 GB |
| Boot volume | 500 GB |
| Workers de transferência iniciais | 4 |

Em 1,2 Gbps, 10 TB levam cerca de 18,5 h no limite teórico; planeje 22–30 h. A retenção padrão de restore deve ser 4 dias.

## Recuperação

PostgreSQL, backups lógicos locais, WAL, logs e releases residem no boot volume persistente. Uma policy automática de backup do boot volume deve ser associada à VM, com retenção mínima de 14 dias. Backup local acelera restauração, mas o backup do volume protege contra perda da VM ou do disco.
