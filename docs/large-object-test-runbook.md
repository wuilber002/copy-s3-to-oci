# Validação controlada de objeto grande

Este roteiro valida multipart, retomada e reconciliação sem usar dados de produção. Ele é manual de propósito: a criação de um objeto de teste na AWS depende de uma identidade que tenha `s3:PutObject`, permissão que a role de migração não deve ter.

## Pré-condições

- Crie um prefixo exclusivo, por exemplo `raijin-validation/<data>/`, no bucket S3 de teste autorizado.
- Selecione um bucket OCI de teste ou o bucket de destino autorizado para o mesmo prefixo.
- Mantenha o worker real habilitado e o worker simulado desabilitado.
- Use um objeto de pelo menos três vezes o tamanho configurado em **Multipart upload part (MiB)**; com 64 MiB, use 192 MiB ou maior.
- Registre o nome da origem, chave e SHA-256 local do arquivo antes do upload para S3.

## Execução

1. Cadastre a origem limitada ao prefixo exclusivo e execute o discovery.
2. Crie uma onda contendo o objeto e coloque-a na fila.
3. Aguarde o primeiro checkpoint multipart aparecer nas métricas (`raijin_active_multipart_checkpoints`).
4. Interrompa somente o serviço `s3-oci-migration.service`, aguarde alguns segundos e inicie-o novamente.
5. Confirme no histórico que a mesma onda continuou e que o objeto não foi marcado como concluído antes do commit OCI.
6. Ao terminar, execute **Validar destino OCI**. O resultado esperado é `VALID`, sem ausências, tamanhos divergentes ou proveniência divergente.
7. Para uma auditoria adicional, execute **Auditoria profunda SHA-256** na onda e confirme o status final auditado.

## Critérios e limpeza

- O objeto deve manter a mesma chave e tamanho no OCI.
- O histórico deve mostrar retomada segura; partes OCI já aceitas não devem ser reenviadas.
- O destino deve preservar ETag e última modificação da versão S3 como metadados de proveniência.
- Ao final, use o [procedimento de cleanup controlado](test-cleanup.md), que exige uma lista de chaves exatas, revisão do plano e confirmação explícita antes de remover qualquer objeto. Depois arquive a origem de teste e registre o resultado no histórico de testes do cliente.

Não use este procedimento em objetos Glacier/Deep Archive sem aprovar previamente o restore e seu custo. O teste de Archive deve seguir o roteiro separado em `docs/validation-test-plan.md`.
