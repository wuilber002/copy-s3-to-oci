# AWS setup — padrão empresarial do Oracle Data Migration Tool

Este guia é a referência canônica para preparar uma conta AWS para uma conexão do **Oracle Data Migration Tool (ODMT)**. O objetivo é conceder o **menor privilégio possível** para descobrir objetos, solicitar restores via S3 Batch Operations, transferir o conteúdo e manter evidências no bucket de controle.

> Não forneça credenciais root, não use `AdministratorAccess` e não reutilize a mesma credencial em contas AWS diferentes. Crie uma conexão ODMT por conta AWS; cada conexão usa um bucket de controle exclusivo.

## 1. Arquitetura e responsabilidades

| Componente | Responsabilidade | Criado por |
|---|---|---|
| Usuário bootstrap | Assume a role de migração. Sua access key/secret é o único segredo AWS entregue ao ODMT. | Cliente AWS |
| Role de migração | Discovery, leitura de objetos, leitura/gravação de artefatos de controle e criação/consulta de jobs Batch. | Cliente AWS |
| Role S3 Batch Operations | Executa `RestoreObject` nos objetos do escopo e grava o completion report. | Cliente AWS |
| Bucket de controle | Armazena manifestos, completion reports e evidências da conexão. | Cliente AWS |
| Bucket de origem | Contém os objetos que serão descobertos e migrados. | Cliente AWS |
| Secret OCI | Guarda o JSON da conexão; nunca é exibido pelo ODMT. | Cliente OCI |

O operador cria os recursos AWS. O Terraform OCI não cria recursos na AWS.

> **Valores a substituir:** todo texto entre sinais de menor e maior, como `<AWS_ACCOUNT_ID>`, é obrigatório e deve ser substituído pelo valor do cliente antes de aplicar a policy. Não altere nomes de ações AWS, efeitos, conditions ou principals de serviço.

## 2. Convenções recomendadas

Use nomes previsíveis e únicos por conta:

| Item | Exemplo recomendado |
|---|---|
| Usuário bootstrap | `oracle-bootstrap-prod` |
| Role de migração | `oracle-s3-migration-role` |
| Role Batch | `oracle-s3-batch-restore-role` |
| Bucket de controle | `oracle-control-bucket-us-east-1` |
| Prefixo de controle da conexão | `oracle/connections/<connection-id>/` |
| Prefixo de uma source | `production/finance/` |

### Informações a registrar antes de criar as policies

Esta é a ficha de parâmetros da conexão. Preencha ou anote os valores abaixo **antes** de montar as policies: os mesmos códigos entre `<...>` serão reutilizados nos ARNs e JSONs das seções seguintes. A tabela é apenas uma referência; não deve ser executada como comando.

| Código usado nas policies | O que informar | Exemplo |
|---|---|---|
| `<AWS_ACCOUNT_ID>` | ID numérico de 12 dígitos da conta AWS que possui as roles. | `123456789012` |
| `<AWS_REGION>` | Região AWS do bucket de origem e do bucket de controle. | `us-east-1` |
| `<SOURCE_BUCKET>` | Nome do bucket S3 de origem, sem `s3://`. | `customer-finance-archive` |
| `<SOURCE_PREFIX>` | Prefixo S3 aprovado para a source; termine com `/` quando for uma pasta lógica. | `production/finance/` |
| `<CONTROL_BUCKET>` | Nome do bucket de controle da conexão, sem `s3://`. | `oracle-control-bucket-us-east-1` |
| `<CONTROL_PREFIX>` | Prefixo exclusivo dos artefatos do ODMT dentro do bucket de controle. | `oracle/` |
| `<BOOTSTRAP_USER>` | Nome do usuário IAM que terá a access key usada pela conexão. | `oracle-bootstrap-prod` |
| `<MIGRATION_ROLE>` | Nome da role assumida pelo ODMT para operar a source. | `oracle-s3-migration-role` |
| `<BATCH_ROLE>` | Nome da role assumida pelo S3 Batch Operations para restores. | `oracle-s3-batch-restore-role` |

Para migrar o bucket inteiro, deixe `SOURCE_PREFIX` vazio e adapte os ARNs de objeto para `arn:aws:s3:::SOURCE_BUCKET/*`. Para vários prefixos, inclua somente os prefixos aprovados nas conditions e nos ARNs; não amplie para o bucket inteiro por conveniência.

O nome de bucket S3 é globalmente único. Use `oracle-control-bucket-us-east-1` quando estiver disponível; se já existir em outra conta AWS, preserve esse padrão e acrescente um sufixo corporativo aprovado, por exemplo `oracle-control-bucket-us-east-1-<AWS_ACCOUNT_ID>`.

## 3. Criar e proteger o bucket de controle

O bucket de controle não recebe dados de negócio; recebe manifestos CSV, relatórios de conclusão e evidências operacionais. Ainda assim, é parte do trilho de auditoria e deve ser tratado como dado operacional sensível.

1. Crie um bucket general purpose na mesma região da source.
2. Habilite **Block Public Access** em todos os quatro controles.
3. Em **Object Ownership**, selecione **Bucket owner enforced** (ACLs desabilitadas).
4. Habilite versionamento.
5. Use SSE-S3 como padrão.
6. Crie lifecycle para expirar artefatos antigos: recomendação inicial de 90 dias, versões não atuais após 35 dias e abortar uploads multipart incompletos após 7 dias.

Exemplo AWS CLI:

```bash
aws s3api create-bucket --bucket "<CONTROL_BUCKET>" --region "<AWS_REGION>"
aws s3api put-public-access-block --bucket "<CONTROL_BUCKET>" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-ownership-controls --bucket "<CONTROL_BUCKET>" \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]'
aws s3api put-bucket-versioning --bucket "<CONTROL_BUCKET>" \
  --versioning-configuration Status=Enabled
```

## 4. Criar o usuário bootstrap

O bootstrap user só pode chamar `sts:AssumeRole` para a role de migração. Não conceda S3, IAM, billing ou console access diretamente a ele.

1. Em **IAM → Users → Create user**, crie `oracle-bootstrap-prod`.
2. Não habilite console access.
3. Crie uma access key de uso programático.
4. Anexe a policy inline abaixo, substituindo conta e role.
5. Armazene o access key ID e secret access key exclusivamente no Secret OCI da conexão.

Substitua no JSON: `<AWS_ACCOUNT_ID>` pelo ID da conta e `<MIGRATION_ROLE>` pelo nome da role criada na próxima seção.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AssumeOnlyOracleMigrationRole",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<MIGRATION_ROLE>"
    }
  ]
}
```

## 5. Criar a role de migração

### 5.1 Trust policy

1. Em **IAM → Roles → Create role**, crie `oracle-s3-migration-role`.
2. Selecione **Custom trust policy**.
3. Cole o documento abaixo, trocando conta e usuário bootstrap.
4. Configure duração máxima de sessão de 1 hora como padrão inicial. A operação re-assume a role quando necessário.

Substitua no JSON: `<AWS_ACCOUNT_ID>` e `<BOOTSTRAP_USER>`. O principal deve ser o usuário IAM criado na seção anterior.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TrustOnlyOracleBootstrapUser",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<AWS_ACCOUNT_ID>:user/<BOOTSTRAP_USER>"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### 5.2 Policy mínima da role de migração

Anexe a policy inline abaixo. Ela cobre o fluxo atualmente usado pelo worker real:

- `ListObjectsV2` no discovery remoto e leitura dos artefatos de controle;
- `HeadObject`, `GetObject` e range GET durante polling, transferência e retomada multipart;
- tags, somente quando a preservação de tags estiver habilitada no ODMT;
- criação e consulta do S3 Batch Operations job;
- `iam:PassRole` limitado ao serviço S3 Batch Operations.

Substitua somente estes valores no JSON:

- `<SOURCE_BUCKET>` e `<SOURCE_PREFIX>` pelo bucket e prefixo que serão cadastrados como source;
- `<CONTROL_BUCKET>` e `<CONTROL_PREFIX>` pelo bucket e prefixo de artefatos da conexão;
- `<AWS_ACCOUNT_ID>` e `<BATCH_ROLE>` pelo destino correto do `iam:PassRole`.

```json
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
      "Resource": "arn:aws:s3:::<SOURCE_BUCKET>",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "<SOURCE_PREFIX>",
            "<SOURCE_PREFIX>*"
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
      "Resource": "arn:aws:s3:::<SOURCE_BUCKET>/<SOURCE_PREFIX>*"
    },
    {
      "Sid": "ReadAndWriteOracleControlArtifacts",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::<CONTROL_BUCKET>"
    },
    {
      "Sid": "ReadAndWriteOracleControlPrefix",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::<CONTROL_BUCKET>/<CONTROL_PREFIX>*"
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
      "Sid": "PassOnlyTheOracleBatchRoleToS3Batch",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<BATCH_ROLE>",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "batchoperations.s3.amazonaws.com"
        }
      }
    }
  ]
}
```

Se tags não forem preservadas, remova as duas ações `GetObject*Tagging`. Não inclua `s3:ListJobs`, `s3:UpdateJobPriority`, `s3:UpdateJobStatus`, `s3:*` ou `Resource: "*"` em S3 apenas para facilitar diagnóstico: eles não são necessários ao fluxo do ODMT.

## 6. Criar a role do S3 Batch Operations

Essa role é assumida pelo serviço AWS S3 Batch Operations, não pelo bootstrap user nem pela VM.

### 6.1 Trust policy

```json
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
```

### 6.2 Policy mínima da role Batch

Substitua no JSON: `<SOURCE_BUCKET>`, `<SOURCE_PREFIX>`, `<CONTROL_BUCKET>` e `<CONTROL_PREFIX>`. Eles devem ser idênticos aos valores usados na policy da role de migração.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RestoreApprovedSourceObjects",
      "Effect": "Allow",
      "Action": "s3:RestoreObject",
      "Resource": "arn:aws:s3:::<SOURCE_BUCKET>/<SOURCE_PREFIX>*"
    },
    {
      "Sid": "ReadOracleManifest",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::<CONTROL_BUCKET>/<CONTROL_PREFIX>*"
    },
    {
      "Sid": "WriteBatchCompletionReport",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::<CONTROL_BUCKET>/<CONTROL_PREFIX>*"
    }
  ]
}
```

Valide que o completion report é habilitado pelo job. O ODMT grava e lê os artefatos em `oracle/`; não use um bucket público para manifestos ou relatórios.

## 7. S3 Inventory (recomendado para inventários grandes)

Para sources com mais de 1 milhão de objetos, prefira S3 Inventory em vez de discovery remoto por API. O ODMT importa CSV UTF-8, opcionalmente GZIP, e somente precisa ler o prefixo de entrega; não precisa criar nem alterar a configuração de Inventory. Alternativamente, o operador pode enviar manualmente um inventário compatível pela interface.

1. No bucket de origem, abra **Management → Inventory configurations → Create inventory configuration**.
2. Nome sugerido: `oracle-daily-inventory`.
3. Selecione frequência **Daily** e formato **CSV** com compressão **GZIP**.
4. Escolha bucket/prefixo de destino dedicado, por exemplo `s3://customer-inventory-reports/oracle/customer-finance-archive/`.
5. Marque ao menos: **Object key**, **Size**, **Last modified date**, **ETag**, **Storage class**. Para bucket versionado, inclua também **Version ID**.
6. Conceda ao serviço S3 permissão de escrita no bucket de destino. Restrinja a policy por `aws:SourceAccount` e `aws:SourceArn`.
7. Na role de migração, adicione somente leitura do prefixo de entrega:

Substitua `<INVENTORY_BUCKET>` e `<INVENTORY_PREFIX>` pelo bucket e prefixo de entrega configurados no S3 Inventory.

```json
{
  "Sid": "ReadApprovedInventoryDeliveryPrefix",
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:GetObjectVersion"
  ],
  "Resource": "arn:aws:s3:::<INVENTORY_BUCKET>/<INVENTORY_PREFIX>*"
}
```

O primeiro relatório não é imediato; aguarde a primeira entrega S3 antes de importar. Não conceda `s3:PutInventoryConfiguration` ao ODMT.

## 8. Acesso entre contas AWS (opcional)

Quando bucket e roles estão em contas distintas:

1. Mantenha bootstrap, migration role e batch role na conta que administrará a conexão.
2. Na conta proprietária do bucket, crie uma bucket policy permitindo à migration role apenas listagem do prefixo e leitura dos objetos; permita à batch role apenas `s3:RestoreObject` no mesmo prefixo.
3. Teste primeiro um prefixo pequeno.

Não substitua os ARNs de objeto por `bucket/*` se a source aprovada for somente um prefixo.

## 9. Criar o Secret OCI e cadastrar a conexão

Depois de criar os recursos AWS:

1. No Vault OCI, crie/atualize uma Secret com o JSON da conexão.
2. Use `connection_name` como etiqueta imutável e amigável; o ODMT usa IDs internos para vínculos e histórico.
3. Informe o `aws_account_id` correto. Ele é validado no pre-check.
4. Use um `control_bucket` exclusivo para essa conta/conexão.
5. Em **Configurações → AWS connections**, execute **Refresh OCI Secrets**, escolha a Secret compatível e registre a conexão.
6. Execute **Pre-check** antes de criar uma source.

Substitua todos os valores entre `<...>`. `connection_name` é o rótulo imutável exibido ao operador; não é usado como identificador interno.

```json
{
  "schema_version": "1",
  "connection_name": "<CONNECTION_LABEL>",
  "aws_account_id": "<AWS_ACCOUNT_ID>",
  "default_region": "<AWS_REGION>",
  "bootstrap_access_key_id": "<BOOTSTRAP_ACCESS_KEY_ID>",
  "bootstrap_secret_access_key": "<BOOTSTRAP_SECRET_ACCESS_KEY>",
  "migration_role_arn": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<MIGRATION_ROLE>",
  "batch_operations_role_arn": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<BATCH_ROLE>",
  "control_bucket": "<CONTROL_BUCKET>"
}
```

Nunca envie o JSON a chat, e-mail, Git, logs ou navegador. O ODMT não exibe nem persiste os dois campos de credencial.

## 10. Validação operacional

Execute esses comandos a partir de uma estação autorizada, usando o perfil do bootstrap user:

```bash
aws sts get-caller-identity
aws sts assume-role \
  --role-arn arn:aws:iam::<AWS_ACCOUNT_ID>:role/<MIGRATION_ROLE> \
  --role-session-name oracle-precheck
```

Com as credenciais temporárias retornadas, valide apenas o escopo aprovado:

```bash
aws s3api list-objects-v2 \
  --bucket "<SOURCE_BUCKET>" \
  --prefix "<SOURCE_PREFIX>" \
  --max-keys 1

aws s3api head-bucket --bucket "<CONTROL_BUCKET>"
```

No ODMT, o **Pre-check** valida identidade AWS e AssumeRole sem listar, restaurar ou copiar objetos. Antes de produção, registre a aprovação das policies, o prefixo autorizado, o bucket de controle e o resultado do pre-check.

Checklist mínimo:

- [ ] Bootstrap user sem console e com uma única ação `sts:AssumeRole`.
- [ ] Trust da migration role aponta somente para o bootstrap user correto.
- [ ] Trust da Batch role aponta somente para `batchoperations.s3.amazonaws.com`.
- [ ] Policies estão limitadas a buckets e prefixos aprovados.
- [ ] `iam:PassRole` está condicionado a `batchoperations.s3.amazonaws.com`.
- [ ] Bucket de controle tem Block Public Access, ownership enforced e lifecycle.
- [ ] Secret OCI foi registrada e o Pre-check passou.

## Referências AWS

- [S3 Batch Operations: IAM roles and policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-iam-role-policies.html)
- [Criar jobs S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-create-job.html)
- [Restore de objetos por S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-initiate-restore-object.html)
- [Relatórios de conclusão do S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-examples-reports.html)
- [Configurar S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/configure-inventory.html)
