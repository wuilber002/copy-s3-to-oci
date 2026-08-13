# Configuração AWS exigida do cliente

Terraform não cria nenhum recurso na AWS. Antes de cadastrar uma origem na interface web, o cliente deve criar os itens abaixo.

Para o procedimento completo, reproduzível e com comandos para AWS CloudShell/AWS CLI, incluindo cleanup dos arquivos temporários e remoção opcional da PoC, consulte [AWS por CLI](aws-cli-setup.md).

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

Insira a access key e a secret access key nos Secrets OCI correspondentes. Não use session token.

## 2. Role de migração

Crie a role indicada no Secret `aws-role-arn`. A trust policy deve autorizar o principal de bootstrap a executar `sts:AssumeRole`.

A policy de permissões da role deve ser limitada ao bucket e prefixo de origem. Inclua `s3:ListBucket`, `s3:GetBucketLocation`, `s3:GetObject`, `s3:GetObjectVersion` quando houver versionamento, e `s3:RestoreObject` para conteúdo Glacier.

## 3. Role do S3 Batch Operations

Crie uma role separada para a S3 Batch Operations iniciar restores. Ela deve confiar em `batchoperations.s3.amazonaws.com`, ter permissão de `s3:RestoreObject` na origem e acesso de leitura ao manifesto e escrita ao relatório de conclusão no bucket de controle.

## 4. Bucket de controle Batch Operations

Disponibilize um bucket S3 general-purpose (ou prefixo reservado) para manifestos e relatórios, por exemplo `s3://<control-bucket>/s3-oci-control/`. A aplicação cria um manifesto imutável por onda e solicita uma única Batch Operations job de restore para ele.

Para buckets versionados, o manifesto inclui `VersionId`, garantindo que o restore e a cópia operem sobre a versão descoberta.

## Criptografia

- SSE-S3: não exige permissão adicional.
- SSE-KMS: inclua `kms:Decrypt` para a role de migração e autorize a role na key policy do KMS.
- SSE-C: não é suportado na primeira versão; não cadastre esta origem até haver suporte explícito.
