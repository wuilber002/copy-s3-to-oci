# AWS por CLI: recursos para a migração S3 → OCI

Este capítulo cria os recursos AWS mínimos para uma origem S3: um usuário de bootstrap com access key, a role de migração, a role de S3 Batch Operations e um bucket de controle para manifestos e relatórios. Execute-o no **AWS CloudShell** ou em uma estação com AWS CLI v2 autenticada na conta que possui o bucket de origem.

> Não execute estes comandos no Terraform/OCI Resource Manager. O Terraform da plataforma não cria recursos na AWS e não recebe credenciais AWS.

## Antes de começar

É necessário ter permissão para criar usuário, access key, roles e policies IAM, além de criar o bucket de controle. Execute este roteiro uma vez por conta AWS. Cada execução gera uma [conexão AWS](aws-connections.md) independente, que pode ser reutilizada por vários sources dessa conta.

Os nomes abaixo são exemplos. O bucket de controle é criado sem versionamento para que o cleanup opcional seja direto. Ele é apenas para manifestos e relatórios; **nunca** coloque os dados de origem nele.

```bash
export AWS_REGION='us-east-1'             # região do bucket de origem
export SOURCE_BUCKET='meu-bucket-origem'  # bucket que contém Glacier/Deep Archive
export SOURCE_PREFIX='dados/'             # vazio para migrar o bucket inteiro

export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export BOOTSTRAP_USER='s3-oci-bootstrap-user'
export MIGRATION_ROLE='s3-oci-migration-role'
export BATCH_ROLE='s3-oci-batch-restore-role'
export CONTROL_BUCKET="s3-oci-control-${AWS_ACCOUNT_ID}-${AWS_REGION}"
export WORKDIR="$(mktemp -d)"

export BOOTSTRAP_USER_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:user/${BOOTSTRAP_USER}"
export MIGRATION_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${MIGRATION_ROLE}"
export BATCH_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${BATCH_ROLE}"
export SOURCE_BUCKET_ARN="arn:aws:s3:::${SOURCE_BUCKET}"
export SOURCE_OBJECT_ARN="${SOURCE_BUCKET_ARN}/${SOURCE_PREFIX}*"
export CONTROL_BUCKET_ARN="arn:aws:s3:::${CONTROL_BUCKET}"
export CONTROL_OBJECT_ARN="${CONTROL_BUCKET_ARN}/raijin/*"

aws sts get-caller-identity
```

O `AWS_ACCOUNT_ID` é obtido pela CLI. Não monte ARNs manualmente: isso evita erro de principal inválido na trust policy.

## 1. Criar e proteger o bucket de controle

O nome de bucket S3 é globalmente único. Se o nome sugerido já existir, escolha outro valor para `CONTROL_BUCKET` e reexecute apenas este bloco.

```bash
if [[ "$AWS_REGION" == 'us-east-1' ]]; then
  aws s3api create-bucket --bucket "$CONTROL_BUCKET" --region "$AWS_REGION"
else
  aws s3api create-bucket \
    --bucket "$CONTROL_BUCKET" \
    --region "$AWS_REGION" \
    --create-bucket-configuration "LocationConstraint=${AWS_REGION}"
fi

aws s3api put-public-access-block \
  --bucket "$CONTROL_BUCKET" \
  --public-access-block-configuration \
  'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'

aws s3api put-bucket-encryption \
  --bucket "$CONTROL_BUCKET" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

Se `create-bucket` retornar `OperationAborted`, aguarde alguns segundos e execute novamente o mesmo comando. Não altere nomes ou policies enquanto a operação anterior estiver em andamento.

## 2. Criar o usuário de bootstrap e a access key

O usuário não recebe acesso direto aos objetos. Ele apenas assume a role de migração. A saída de `create-access-key` contém a única cópia da secret access key: copie os dois valores diretamente para as Secrets OCI e não os grave em arquivo ou no repositório.

```bash
aws iam create-user --user-name "$BOOTSTRAP_USER"

cat >"$WORKDIR/bootstrap-policy.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Resource": "${MIGRATION_ROLE_ARN}"
  }]
}
EOF

aws iam put-user-policy \
  --user-name "$BOOTSTRAP_USER" \
  --policy-name 's3-oci-assume-migration-role' \
  --policy-document "file://$WORKDIR/bootstrap-policy.json"

aws iam create-access-key \
  --user-name "$BOOTSTRAP_USER" \
  --query 'AccessKey.[AccessKeyId,SecretAccessKey]' \
  --output text
```

Guarde os dois valores somente durante este roteiro, em local seguro. Ao final, eles serão incluídos em **um único Secret JSON de conexão AWS**; não use mais as antigas Secrets separadas `aws_access_key_id` e `aws_secret_access_key`.

## 3. Criar a role de migração

Essa role é assumida pelo usuário de bootstrap e será usada pelo worker da plataforma para discovery, criação do manifesto, criação/consulta de Batch Jobs e leitura dos objetos restaurados. As ações de S3 Batch Operations de controle não aceitam restrição a um ARN de objeto; por isso permanecem em `Resource: "*"`, enquanto o acesso aos buckets é restrito aos ARNs definidos acima.

```bash
cat >"$WORKDIR/migration-trust.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "${BOOTSTRAP_USER_ARN}"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
  --role-name "$MIGRATION_ROLE" \
  --assume-role-policy-document "file://$WORKDIR/migration-trust.json"

cat >"$WORKDIR/migration-policy.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListOnlyTheConfiguredSourcePrefix",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "${SOURCE_BUCKET_ARN}",
      "Condition": {"StringLike": {"s3:prefix": ["${SOURCE_PREFIX}*"]}}
    },
    {
      "Sid": "ReadSourceObjectsAndTags",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectTagging", "s3:GetObjectVersionTagging"],
      "Resource": "${SOURCE_OBJECT_ARN}"
    },
    {
      "Sid": "LocateControlBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "${CONTROL_BUCKET_ARN}"
    },
    {
      "Sid": "WriteManifestAndReadBatchReports",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
      "Resource": "${CONTROL_OBJECT_ARN}"
    },
    {
      "Sid": "CreateAndMonitorOnlyBatchJobs",
      "Effect": "Allow",
      "Action": ["s3:CreateJob", "s3:DescribeJob", "s3:ListJobs", "s3:UpdateJobPriority", "s3:UpdateJobStatus"],
      "Resource": "*"
    },
    {
      "Sid": "PassOnlyTheBatchRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "${BATCH_ROLE_ARN}"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name "$MIGRATION_ROLE" \
  --policy-name 's3-oci-migration' \
  --policy-document "file://$WORKDIR/migration-policy.json"
```

`LocateControlBucket` é obrigatório mesmo quando a aplicação só grava manifestos sob um prefixo: o pré-check usa `HeadBucket` sem escrita e exige `s3:ListBucket` no ARN do bucket. Não adicione `s3:ListBucket` ao ARN de objetos nem conceda `s3:RestoreObject` a esta role; o restore é executado exclusivamente pela role de S3 Batch Operations abaixo.

Guarde o ARN retornado para compor o Secret JSON de conexão AWS ao final:

```bash
aws iam get-role --role-name "$MIGRATION_ROLE" --query 'Role.Arn' --output text
```

## 4. Criar a role do S3 Batch Operations

Essa é uma role diferente. O serviço S3 Batch Operations a assume para solicitar restores em lote, ler o manifesto e gravar o relatório. Ela não deve ser usada pela aplicação diretamente.

```bash
cat >"$WORKDIR/batch-trust.json" <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "batchoperations.s3.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
  --role-name "$BATCH_ROLE" \
  --assume-role-policy-document "file://$WORKDIR/batch-trust.json"

cat >"$WORKDIR/batch-policy.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RestoreOnlyConfiguredSourceObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:RestoreObject"],
      "Resource": "${SOURCE_OBJECT_ARN}"
    },
    {
      "Sid": "LocateControlBucket",
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "${CONTROL_BUCKET_ARN}"
    },
    {
      "Sid": "ReadManifestAndWriteReports",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
      "Resource": "${CONTROL_OBJECT_ARN}"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name "$BATCH_ROLE" \
  --policy-name 's3-oci-batch-restore' \
  --policy-document "file://$WORKDIR/batch-policy.json"

aws iam get-role --role-name "$BATCH_ROLE" --query 'Role.Arn' --output text
```

Guarde esse ARN para compor o Secret JSON de conexão AWS ao final.

## 5. Verificar e validar na plataforma

Verifique os recursos sem listar objetos nem iniciar restore:

```bash
aws iam get-role --role-name "$MIGRATION_ROLE" --query 'Role.AssumeRolePolicyDocument'
aws iam get-role --role-name "$BATCH_ROLE" --query 'Role.AssumeRolePolicyDocument'
aws s3api head-bucket --bucket "$CONTROL_BUCKET" --region "$AWS_REGION"
```

Crie um Secret OCI com o schema descrito em [Conexões AWS](aws-connections.md), preenchendo `aws_account_id`, região, access key, secret key, os dois ARNs e `CONTROL_BUCKET`. Depois, na interface, acesse **Configurações → Conexões AWS**, atualize a descoberta, cadastre o Secret e execute o pré-check da conexão. Ele faz somente `GetCallerIdentity`, `AssumeRole` e `HeadBucket` no bucket de controle; não lista objetos de origem, restaura, baixa ou envia dados.

## Cleanup ao terminar o procedimento

Remova os arquivos temporários e variáveis da sessão CloudShell assim que as Secrets OCI forem preenchidas. Isso não remove nenhum recurso AWS em uso:

```bash
rm -rf "$WORKDIR"
unset AWS_SECRET_ACCESS_KEY AWS_ACCESS_KEY_ID
unset BOOTSTRAP_USER_ARN MIGRATION_ROLE_ARN BATCH_ROLE_ARN
unset SOURCE_BUCKET_ARN SOURCE_OBJECT_ARN CONTROL_BUCKET_ARN CONTROL_OBJECT_ARN
```

### Remoção completa de um ambiente de validação (opcional e destrutiva)

Execute **somente depois de encerrar a migração** e remover/substituir as versões das Secrets OCI. Os comandos abaixo removem a access key, usuário, roles e o bucket de controle identificados pelas variáveis desta página. Eles não tocam no bucket de origem.

```bash
BOOTSTRAP_ACCESS_KEY_ID="$(aws iam list-access-keys \
  --user-name "$BOOTSTRAP_USER" \
  --query 'AccessKeyMetadata[0].AccessKeyId' --output text)"

[[ "$BOOTSTRAP_ACCESS_KEY_ID" != 'None' ]] && \
  aws iam delete-access-key --user-name "$BOOTSTRAP_USER" --access-key-id "$BOOTSTRAP_ACCESS_KEY_ID"

aws iam delete-user-policy --user-name "$BOOTSTRAP_USER" --policy-name 's3-oci-assume-migration-role'
aws iam delete-user --user-name "$BOOTSTRAP_USER"
aws iam delete-role-policy --role-name "$MIGRATION_ROLE" --policy-name 's3-oci-migration'
aws iam delete-role --role-name "$MIGRATION_ROLE"
aws iam delete-role-policy --role-name "$BATCH_ROLE" --policy-name 's3-oci-batch-restore'
aws iam delete-role --role-name "$BATCH_ROLE"

# Remove somente o bucket de controle criado por este procedimento e seu conteúdo.
aws s3 rb "s3://${CONTROL_BUCKET}" --force
```

Se o bucket de controle tiver versionamento habilitado manualmente, remova versões e delete markers antes de executar `aws s3 rb`. Para regras de permissão e comportamento de S3 Batch Operations, consulte a documentação oficial da AWS sobre [IAM para Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-iam-role-policies.html) e [criação de buckets via CLI](https://docs.aws.amazon.com/cli/latest/reference/s3api/create-bucket.html).
