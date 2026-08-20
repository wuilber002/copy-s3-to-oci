# Configuração AWS exigida do cliente

Terraform não cria nenhum recurso na AWS. Antes de cadastrar uma origem na interface web, o cliente deve criar os itens abaixo.

Para o procedimento completo, reproduzível e com comandos para AWS CloudShell/AWS CLI, incluindo cleanup dos arquivos temporários e remoção opcional do ambiente de validação, consulte [AWS por CLI](aws-cli-setup.md).

## 1. Credencial de bootstrap

Crie um IAM user ou principal de automação com uma access key de longa duração, limitada exclusivamente a assumir a role da migração:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/<MIGRATION_ROLE_NAME>"
  }]
}
```

Insira a access key e a secret access key no único Secret JSON da [conexão AWS](aws-connections.md). Não use session token.

## 2. Role de migração

Crie a role cujo ARN será inserido no campo `migration_role_arn` do Secret JSON. A trust policy deve autorizar o principal de bootstrap a executar `sts:AssumeRole`.

A policy de permissões da role deve ser limitada ao bucket e prefixo de origem. Inclua `s3:ListBucket`, `s3:GetBucketLocation`, `s3:GetObject`, `s3:GetObjectVersion` quando houver versionamento, `s3:GetObjectTagging` e `s3:RestoreObject` para conteúdo Glacier. Quando usar **S3 Inventory por manifest**, inclua também `s3:GetObject` somente no bucket e prefixo de entrega que contém o `manifest.json` e seus shards CSV/GZIP; o Raijin lê esses arquivos pelo worker e não pelo navegador.

## 3. Role do S3 Batch Operations

Crie uma role separada para a S3 Batch Operations iniciar restores e informe seu ARN em `batch_operations_role_arn` no Secret JSON. Ela deve confiar em `batchoperations.s3.amazonaws.com`, ter permissão de `s3:RestoreObject` na origem e acesso de leitura ao manifesto e escrita ao relatório de conclusão no bucket de controle.

## 4. Bucket de controle Batch Operations

Disponibilize um bucket S3 general-purpose exclusivo por conexão AWS para manifestos e relatórios. A aplicação cria internamente caminhos como `raijin/connections/<id>/sources/<id>/waves/<id>/`, evitando mistura entre sources sem depender de prefixos digitados pelo operador.

Para buckets versionados, o manifesto inclui `VersionId`, garantindo que o restore e a cópia operem sobre a versão descoberta.

## Criptografia

- SSE-S3: não exige permissão adicional.
- SSE-KMS: inclua `kms:Decrypt` para a role de migração e autorize a role na key policy do KMS.
- SSE-C: não é suportado na primeira versão; não cadastre esta origem até haver suporte explícito.
