# Plano de validação — Linux 2.6.12-rc2

## Dataset

- origem: S3 Glacier Deep Archive;
- 17.277 arquivos;
- 1.075 prefixes lógicos;
- aproximadamente 220 MB.

## Objetivo

Validar o ciclo completo sem gerar custo de recuperação durante discovery e antes de executar ondas reais de até 10 TB.

## Parâmetros iniciais

| Item | Valor |
| --- | --- |
| Tamanho máximo da onda | 10 TB; a validação usa todo o dataset de 220 MB |
| Tier de restore | Bulk |
| Retenção | 3 dias |
| Workers de transferência | 4 |
| Integridade | SHA-256 obrigatório e MD5 complementar |

## Roteiro

1. Cadastrar a origem, a role AWS de migração e o bucket/prefixo de controle de S3 Batch Operations.
2. Executar discovery e registrar quantidade, tamanhos, chaves, ETag, classe, última modificação, duração e páginas `ListObjectsV2` retornadas pela console.
3. Conferir que discovery não executou `GetObject`, `RestoreObject` ou `HeadObject` por objeto.
4. Criar uma onda contendo todo o inventário e gerar o manifesto CSV imutável.
5. Criar uma única S3 Batch Operations Restore job em modo Bulk.
6. Acompanhar a job com backoff; depois acompanhar disponibilidade pelo método escolhido pelo Raijin: `HeadObject` para wave pequena ou listagens paginadas com `RestoreStatus` para inventário maior.
7. Durante o restore, reiniciar a VM uma vez e confirmar que a job e o estado local são retomados sem criar uma nova job de restore. Em uma source de teste sem waves, interrompa também o discovery entre páginas e confirme que a retomada continua no checkpoint salvo.
8. Transferir os objetos restaurados, capturar metadados/tags e calcular SHA-256 durante a leitura. Validar SHA-256 no OCI durante o `PutObject` ou em cada parte do multipart, sem releitura do destino.
9. Executar auditoria profunda em uma onda pequena: confirmar o aviso reforçado, acompanhar o progresso no Status e comparar o SHA-256 linear por leitura completa do OCI.
9. Durante a transferência, reiniciar a VM uma vez; confirmar retomada idempotente, sem marcar objetos incompletos como concluídos.
10. Executar reconciliação final.

## Critérios de aprovação

- 17.277 objetos descobertos e 17.277 objetos verificados no OCI;
- soma de bytes de origem e destino idêntica;
- SHA-256 idêntico por objeto;
- chaves dos objetos preservadas;
- metadata/tags preservados no objeto OCI quando compatíveis e sempre preservados no manifesto;
- uma única Batch Operations job de restore para a onda;
- nenhuma chamada de recuperação no discovery;
- recuperação comprovada após os dois reinícios planejados;
- nenhum item pendente, `VALIDATION_FAILED` ou `SOURCE_CHANGED` ao encerrar.
