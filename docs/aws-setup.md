# AWS setup — padrão empresarial do RAIJIN

Este guia é a referência canônica para preparar uma conta AWS para uma conexão do RAIJIN. O objetivo é conceder o **menor privilégio possível** para descobrir objetos, solicitar restores via S3 Batch Operations, transferir o conteúdo e manter as evidências no bucket de controle.

> Não forneça credenciais root, não use \`AdministratorAccess\` e não reutilize a mesma credencial em contas AWS diferentes. Crie uma conexão RAIJIN por conta AWS; cada conexão usa um bucket de controle exclusivo.

## 1. Arquitetura e responsabilidades

| Componente | Responsabilidade | Criado por |
|---|---|---|
| Usuário bootstrap | Assume a role de migração. Sua access key/secret é o único segredo AWS entregue ao RAIJIN. | Cliente AWS |
| Role de migração | Discovery, leitura de objetos, leitura/gravação de artefatos de controle e criação/consulta de jobs Batch. | Cliente AWS |
| Role S3 Batch Operations | Executa \`RestoreObject\` nos objetos do escopo e grava o completion report. | Cliente AWS |
| Bucket de controle | Armazena manifestos, completion reports e evidências da conexão. | Cliente AWS |
| Bucket de origem | Contém os objetos que serão descobertos e migrados. | Cliente AWS |
| Secret OCI | Guarda o JSON da conexão; nunca é exibido pelo RAIJIN. | Cliente OCI |

O operador cria os recursos AWS. O Terraform OCI não cria recursos na AWS.

## 2. Convenções recomendadas

Use nomes previsíveis e únicos por conta:

| Item | Exemplo recomendado |
|---|---|
| Usuário bootstrap | \`raijin-bootstrap-prod\` |
| Role de migração | \`raijin-s3-migration-role\` |
| Role Batch | \`raijin-s3-batch-restore-role\` |
| Bucket de controle | \`raijin-control-123456789012-us-east-1\` |
| Prefixo de controle da conexão | \`raijin/connections/<connection-id>/\` |
| Prefixo de uma source | \`production/finance/\` |

Antes de criar policies, registre:

\`\`\`text
AWS_ACCOUNT_ID        = 123456789012
AWS_REGION            = us-east-1
SOURCE_BUCKET         = customer-finance-archive
SOURCE_PREFIX         = production/finance/
CONTROL_BUCKET        = raijin-control-123456789012-us-east-1
BOOTSTRAP_USER        = raijin-bootstrap-prod
MIGRATION_ROLE        = raijin-s3-migration-role
BATCH_ROLE            = raijin-s3-batch-restore-role
\`\`\`

Para migrar o bucket inteiro, deixe \`SOURCE_PREFIX\` vazio e adapte os ARNs de objeto para \`arn:aws:s3:::SOURCE_BUCKET/*\`. Para vários prefixos, inclua somente os prefixos aprovados nas conditions e nos ARNs; não amplie para o bucket inteiro por conveniência.

## 3. Criar e proteger o bucket de controle

O bucket de controle não recebe dados de negócio; recebe manifestos CSV, relatórios de conclusão e evidências operacionais. Ainda assim, é parte do trilho de auditoria e deve ser tratado como dado operacional sensível.

1. Crie um bucket general purpose na mesma região da source.
2. Habilite **Block Public Access** em todos os quatro controles.
3. Em **Object Ownership**, selecione **Bucket owner enforced** (ACLs desabilitadas).
4. Habilite versionamento.
5. Use SSE-S3 como padrão. Se houver exigência de CMK, siga também a seção [KMS](#8-criptografia-kms-opcional).
6. Crie lifecycle para expirar artefatos antigos: recomendação inicial de 90 dias, versões não atuais após 35 dias e abortar uploads multipart incompletos após 7 dias.
7. Ative CloudTrail data events para o bucket de controle quando a política corporativa exigir trilha detalhada.

Exemplo AWS CLI:

\`\`\`bash
aws s3api create-bucket --bucket raijin-control-123456789012-us-east-1 --region us-east-1
aws s3api put-public-access-block --bucket raijin-control-123456789012-us-east-1 \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-ownership-controls --bucket raijin-control-123456789012-us-east-1 \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]'
aws s3api put-bucket-versioning --bucket raijin-control-123456789012-us-east-1 \
  --versioning-configuration Status=Enabled
\`\`\`

## 4. Criar o usuário bootstrap

O bootstrap user só pode chamar \`sts:AssumeRole\` para a role de migração. Não conceda S3, IAM, billing ou console access diretamente a ele.

1. Em **IAM → Users → Create user**, crie \`raijin-bootstrap-prod\`.
2. Não habilite console access.
3. Crie uma access key de uso programático.
4. Anexe a policy inline abaixo, substituindo conta e role.
5. Armazene o access key ID e secret access key exclusivamente no Secret OCI da conexão.
6. Defina rotação corporativa; recomendação inicial: 90 dias, com validação da nova versão do Secret antes de revogar a anterior.

\`\`\`json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AssumeOnlyRaijinMigrationRole",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::123456789012:role/raijin-s3-migration-role"
    }
  ]
}
\`\`\`

## 5. Criar a role de migração

### 5.1 Trust policy

1. Em **IAM → Roles → Create role**, crie \`raijin-s3-migration-role\`.
2. Selecione **Custom trust policy**.
3. Cole o documento abaixo, trocando conta e usuário bootstrap.
4. Configure duração máxima de sessão de 1 hora como padrão inicial. A operação re-assume a role quando necessário.

\`\`\`json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustOnlyRaijinBootstrapUser",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:user/raijin-bootstrap-prod"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
\`\`\`

### 5.2 Policy mínima da role de migração

Anexe a policy inline abaixo. Ela cobre o fluxo atualmente usado pelo worker real:

- \`ListObjectsV2\` no discovery remoto e leitura dos artefatos de controle;
- \`HeadObject\`, \`GetObject\` e range GET durante polling, transferência e retomada multipart;
- tags, somente quando a preservação de tags estiver habilitada no RAIJIN;
- criação e consulta do S3 Batch Operations job;
- \`iam:PassRole\` limitado ao serviço S3 Batch Operations.

\`\`\`json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DiscoverApprovedSourcePrefix",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::customer-finance-archive",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "production/finance/",
            "production/finance/*"
          ]
        }
      }
    },
    {
      "Sid": "ReadApprovedSourceObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetObjectTagging",
        "s3:GetObjectVersionTagging"
      ],
      "Resource": "arn:aws:s3:::customer-finance-archive/production/finance/*"
    },
    {
      "Sid": "ReadAndWriteRaijinControlArtifacts",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::raijin-control-123456789012-us-east-1"
    },
    {
      "Sid": "ReadAndWriteRaijinControlPrefix",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::raijin-control-123456789012-us-east-1/raijin/*"
    },
    {
      "Sid": "CreateAndInspectOwnBatchJobs",
      "Effect": "Allow",
      "Action": [
        "s3:CreateJob",
        "s3:DescribeJob"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassOnlyTheRaijinBatchRoleToS3Batch",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::123456789012:role/raijin-s3-batch-restore-role",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "batchoperations.s3.amazonaws.com"
        }
      }
    }
  ]
}
\`\`\`

Se tags não forem preservadas, remova as duas ações \`GetObject*Tagging\`. Não inclua \`s3:ListJobs\`, \`s3:UpdateJobPriority\`, \`s3:UpdateJobStatus\`, \`s3:*\` ou \`Resource: "*"\` em S3 apenas para facilitar diagnóstico: eles não são necessários ao fluxo do RAIJIN.

## 6. Criar a role do S3 Batch Operations

Essa role é assumida pelo serviço AWS S3 Batch Operations, não pelo bootstrap user nem pela VM.

### 6.1 Trust policy

\`\`\`json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustS3BatchOperationsOnly",
      "Effect": "Allow",
      "Principal": {
        "Service": "batchoperations.s3.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
\`\`\`

### 6.2 Policy mínima da role Batch

\`\`\`json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RestoreApprovedSourceObjects",
      "Effect": "Allow",
      "Action": "s3:RestoreObject",
      "Resource": "arn:aws:s3:::customer-finance-archive/production/finance/*"
    },
    {
      "Sid": "ReadRaijinManifest",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::raijin-control-123456789012-us-east-1/raijin/*"
    },
    {
      "Sid": "WriteBatchCompletionReport",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::raijin-control-123456789012-us-east-1/raijin/*"
    }
  ]
}
\`\`\`

Valide que o completion report é habilitado pelo job. O RAIJIN grava e lê os artefatos em \`raijin/\`; não use um bucket público para manifestos ou relatórios.

## 7. S3 Inventory (recomendado para inventários grandes)

Para sources com mais de 1 milhão de objetos, prefira S3 Inventory em vez de discovery remoto por API. O RAIJIN importa CSV UTF-8, opcionalmente GZIP, e somente precisa ler o prefixo de entrega; não precisa criar nem alterar a configuração de Inventory.

1. No bucket de origem, abra **Management → Inventory configurations → Create inventory configuration**.
2. Nome sugerido: \`raijin-daily-inventory\`.
3. Selecione frequência **Daily** e formato **CSV** com compressão **GZIP**.
4. Escolha bucket/prefixo de destino dedicado, por exemplo \`s3://customer-inventory-reports/raijin/customer-finance-archive/\`.
5. Marque ao menos: **Object key**, **Size**, **Last modified date**, **ETag**, **Storage class**. Para bucket versionado, inclua também **Version ID**.
6. Conceda ao serviço S3 permissão de escrita no bucket de destino. Restrinja a policy por \`aws:SourceAccount\` e \`aws:SourceArn\`.
7. Na role de migração, adicione somente leitura do prefixo de entrega:

\`\`\`json
{
  "Sid": "ReadApprovedInventoryDeliveryPrefix",
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:GetObjectVersion"
  ],
  "Resource": "arn:aws:s3:::customer-inventory-reports/raijin/customer-finance-archive/*"
}
\`\`\`

O primeiro relatório não é imediato; aguarde a primeira entrega S3 antes de importar. Não conceda \`s3:PutInventoryConfiguration\` ao RAIJIN.

## 8. Criptografia KMS (opcional)

SSE-S3 não requer policy adicional. Se source, Inventory ou controle usam SSE-KMS com CMK, configure **IAM policy e key policy**: uma permissão IAM isolada não basta quando a key policy não delega o acesso.

| Recurso criptografado | Principal | Permissões mínimas típicas |
|---|---|---|
| Objetos de origem | Role de migração | \`kms:Decrypt\` |
| Manifesto Inventory | Role de migração | \`kms:Decrypt\`, \`kms:GenerateDataKey\` quando exigido pelo fluxo |
| Manifesto e report Batch | Role Batch | \`kms:Decrypt\`, \`kms:GenerateDataKey\` |
| Artefatos de controle | Role de migração e, quando aplicável, role Batch | \`kms:Encrypt\`, \`kms:Decrypt\`, \`kms:GenerateDataKey\` |

Restrinja a key policy aos ARNs das duas roles e, quando possível, ao contexto de criptografia S3 e ao bucket. SSE-C não é suportado como padrão operacional do RAIJIN.

## 9. Acesso entre contas AWS (opcional)

Quando bucket e roles estão em contas distintas:

1. Mantenha bootstrap, migration role e batch role na conta que administrará a conexão.
2. Na conta proprietária do bucket, crie uma bucket policy permitindo à migration role apenas listagem do prefixo e leitura dos objetos; permita à batch role apenas \`s3:RestoreObject\` no mesmo prefixo.
3. Se houver CMK, altere também a key policy da conta proprietária.
4. Teste primeiro um prefixo pequeno.

Não substitua os ARNs de objeto por \`bucket/*\` se a source aprovada for somente um prefixo.

## 10. Criar o Secret OCI e cadastrar a conexão

Depois de criar os recursos AWS:

1. No Vault OCI, crie/atualize uma Secret com o JSON da conexão.
2. Use \`connection_name\` como etiqueta imutável e amigável; o RAIJIN usa IDs internos para vínculos e histórico.
3. Informe o \`aws_account_id\` correto. Ele é validado no pre-check.
4. Use um \`control_bucket\` exclusivo para essa conta/conexão.
5. Em **Configurações → AWS connections**, execute **Refresh OCI Secrets**, escolha a Secret compatível e registre a conexão.
6. Execute **Pre-check** antes de criar uma source.

\`\`\`json
{
  "schema_version": "1",
  "connection_name": "Financeiro Produção",
  "aws_account_id": "123456789012",
  "default_region": "us-east-1",
  "bootstrap_access_key_id": "AKIA...",
  "bootstrap_secret_access_key": "substitua-por-um-segredo",
  "migration_role_arn": "arn:aws:iam::123456789012:role/raijin-s3-migration-role",
  "batch_operations_role_arn": "arn:aws:iam::123456789012:role/raijin-s3-batch-restore-role",
  "control_bucket": "raijin-control-123456789012-us-east-1"
}
\`\`\`

Nunca envie o JSON a chat, e-mail, Git, logs ou navegador. O RAIJIN não exibe nem persiste os dois campos de credencial.

## 11. Validação operacional e auditoria

Execute esses comandos a partir de uma estação autorizada, usando o perfil do bootstrap user:

\`\`\`bash
aws sts get-caller-identity
aws sts assume-role \
  --role-arn arn:aws:iam::123456789012:role/raijin-s3-migration-role \
  --role-session-name raijin-precheck
\`\`\`

Com as credenciais temporárias retornadas, valide apenas o escopo aprovado:

\`\`\`bash
aws s3api list-objects-v2 \
  --bucket customer-finance-archive \
  --prefix production/finance/ \
  --max-keys 1

aws s3api head-bucket --bucket raijin-control-123456789012-us-east-1
\`\`\`

No RAIJIN, o **Pre-check** valida identidade AWS e AssumeRole sem listar, restaurar ou copiar objetos. Antes de produção, registre a aprovação das policies, o prefixo autorizado, o bucket de controle e o resultado do pre-check.

Checklist mínimo:

- [ ] Bootstrap user sem console e com uma única ação \`sts:AssumeRole\`.
- [ ] Trust da migration role aponta somente para o bootstrap user correto.
- [ ] Trust da Batch role aponta somente para \`batchoperations.s3.amazonaws.com\`.
- [ ] Policies estão limitadas a buckets e prefixos aprovados.
- [ ] \`iam:PassRole\` está condicionado a \`batchoperations.s3.amazonaws.com\`.
- [ ] Bucket de controle tem Block Public Access, ownership enforced e lifecycle.
- [ ] KMS/key policies foram validadas, quando aplicável.
- [ ] Secret OCI foi registrada e o Pre-check passou.
- [ ] CloudTrail/monitoramento e rotação de chave atendem à política corporativa.

## 12. Resultado da auditoria do ambiente de teste

A auditoria de 26/08/2026 confirmou no ambiente de testes que:

- o bootstrap user assume somente a migration role;
- a migration role confia somente no bootstrap user;
- a role Batch confia somente em S3 Batch Operations;
- as permissões essenciais de discovery, leitura, controle, Batch restore e \`PassRole\` existem.

Também foi encontrada uma policy adicional de \`s3:ListBucket\` no bucket de teste sem condition de prefixo, criada para validações anteriores. Ela **não** integra o padrão empresarial deste documento e deve ser removida ou reduzida ao prefixo aprovado antes de produção.

## Referências AWS

- [S3 Batch Operations: IAM roles and policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-iam-role-policies.html)
- [Criar jobs S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-create-job.html)
- [Restore de objetos por S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-initiate-restore-object.html)
- [Relatórios de conclusão do S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-examples-reports.html)
- [Configurar S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/configure-inventory.html)
