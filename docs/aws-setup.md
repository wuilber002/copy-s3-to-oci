# AWS Setup

Este guia é a referência para preparar uma conta da Amazon Web Services (AWS) para uma conexão de migração. O objetivo é conceder o **menor privilégio possível** para descobrir objetos, solicitar restaurações por meio do *S3 Batch Operations*, transferir o conteúdo e manter evidências no *bucket* de controle.

> Não forneça credenciais da conta raiz, não use a política gerenciada `AdministratorAccess` e não reutilize a mesma credencial em contas AWS distintas. Crie uma conexão por conta AWS; cada conexão deve usar um *bucket* de controle exclusivo.

## 1. Arquitetura e responsabilidades

| Componente | Responsabilidade | Criado por |
|---|---|---|
| Usuário de inicialização (*bootstrap*) | Assume a função (*role*) de migração. Sua chave de acesso e chave secreta são os únicos segredos AWS entregues à aplicação. | Cliente AWS |
| Função de migração | Descoberta, leitura de objetos, leitura e gravação de artefatos de controle, além da criação e consulta de tarefas do *S3 Batch Operations*. | Cliente AWS |
| Função do *S3 Batch Operations* | Executa `RestoreObject` nos objetos do escopo e grava o relatório de conclusão. | Cliente AWS |
| *Bucket* de controle | Armazena manifestos, relatórios de conclusão e evidências da conexão. | Cliente AWS |
| *Bucket* de origem | Contém os objetos que serão descobertos e migrados. | Cliente AWS |
| Segredo no OCI | Armazena o JSON da conexão; seu conteúdo nunca é exibido pela aplicação. | Cliente OCI |

O operador cria os recursos AWS diretamente na conta AWS.

> **Valores a substituir:** todo texto entre sinais de menor e maior, como `<AWS_ACCOUNT_ID>`, é obrigatório e deve ser substituído pelo valor do cliente antes de aplicar a política. Não altere nomes de ações AWS, efeitos, condições ou entidades principais (*principals*) de serviço.

## 2. Convenções recomendadas

Use nomes previsíveis e únicos por conta:

| Item | Exemplo recomendado |
|---|---|
| Usuário de inicialização | `oracle-bootstrap-prod` |
| Função de migração | `oracle-s3-migration-role` |
| Função do *Batch* | `oracle-s3-batch-restore-role` |
| *Bucket* de controle | `oracle-control-bucket-us-east-1` |

### Informações a registrar antes de criar as políticas

Esta é a ficha de parâmetros da conexão. Registre os valores abaixo **antes** de montar as políticas: os mesmos códigos entre `<...>` serão reutilizados nos ARNs e JSONs das seções seguintes. A tabela não é um comando.

| Código usado nas políticas | O que informar | Exemplo |
|---|---|---|
| `<AWS_ACCOUNT_ID>` | Identificador numérico de 12 dígitos da conta AWS que possui as funções. | `123456789012` |
| `<AWS_REGION>` | Região AWS do *bucket* de origem e do *bucket* de controle. | `us-east-1` |
| `<SOURCE_BUCKET>` | Nome do *bucket* Amazon S3 de origem, sem `s3://`. | `customer-finance-archive` |
| `<SOURCE_PREFIX>` | Prefixo Amazon S3 aprovado para a origem; termine com `/` quando representar uma pasta lógica. | `production/finance/` |
| `<CONTROL_BUCKET>` | Nome do *bucket* de controle da conexão, sem `s3://`. | `oracle-control-bucket-us-east-1` |
| `<BOOTSTRAP_USER>` | Nome do usuário IAM cuja chave de acesso será usada pela conexão. | `oracle-bootstrap-prod` |
| `<MIGRATION_ROLE>` | Nome da função de operação da origem. | `oracle-s3-migration-role` |
| `<BATCH_ROLE>` | Nome da função assumida pelo *S3 Batch Operations* para as restaurações. | `oracle-s3-batch-restore-role` |

Para migrar o *bucket* inteiro, deixe `SOURCE_PREFIX` vazio e adapte os ARNs de objeto para `arn:aws:s3:::SOURCE_BUCKET/*`. Para vários prefixos, inclua somente os prefixos aprovados nas condições e nos ARNs; não amplie o escopo para o *bucket* inteiro por conveniência.

O nome de um *bucket* Amazon S3 é globalmente único. Use `oracle-control-bucket-us-east-1` quando estiver disponível; se ele já existir em outra conta AWS, preserve esse padrão e acrescente um sufixo corporativo aprovado, por exemplo, `oracle-control-bucket-us-east-1-<AWS_ACCOUNT_ID>`.

## 3. Criar e proteger o *bucket* de controle

O *bucket* de controle não recebe dados de negócio; ele recebe manifestos em CSV, relatórios de conclusão e evidências operacionais. Ainda assim, integra o trilho de auditoria e deve ser tratado como dado operacional sensível.

1. Crie um *bucket* de finalidade geral na mesma região da origem.
2. Habilite **Block Public Access** nos quatro controles disponíveis.
3. Em **Object Ownership**, selecione **Bucket owner enforced** (listas de controle de acesso, ou *access control lists* — ACLs, desabilitadas).
4. Habilite versionamento.
5. Use a criptografia SSE-S3 como padrão.
6. Crie uma regra de ciclo de vida (*lifecycle*) para expirar artefatos antigos: a recomendação inicial é de 90 dias, com exclusão de versões não atuais após 35 dias e cancelamento de envios *multipart* incompletos após 7 dias.

Exemplo com a AWS CLI:

```bash
aws s3api create-bucket --bucket "<CONTROL_BUCKET>" --region "<AWS_REGION>"
aws s3api put-public-access-block --bucket "<CONTROL_BUCKET>" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-ownership-controls --bucket "<CONTROL_BUCKET>" \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]'
aws s3api put-bucket-versioning --bucket "<CONTROL_BUCKET>" \
  --versioning-configuration Status=Enabled
```

## 4. Criar o usuário de inicialização

O usuário de inicialização (*bootstrap user*) só pode chamar `sts:AssumeRole` para a função de migração. Não conceda permissões do Amazon S3, IAM, faturamento (*billing*) ou acesso ao console diretamente a esse usuário.

1. Em **IAM → Users → Create user**, crie `oracle-bootstrap-prod`.
2. Não habilite acesso ao console.
3. Crie uma chave de acesso (*access key*) de uso programático.
4. Anexe a política em linha (*inline policy*) abaixo, substituindo conta e função.
5. Armazene o identificador da chave de acesso (*access key ID*) e a chave secreta (*secret access key*) exclusivamente no segredo da conexão no OCI.

Substitua no JSON: `<AWS_ACCOUNT_ID>` pelo identificador da conta e `<MIGRATION_ROLE>` pelo nome da função criada na próxima seção.

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

### Procedimento equivalente com a AWS CLI

Salve o JSON anterior como `bootstrap-assume-role-policy.json` e execute os comandos abaixo. A criação da chave de acesso retorna a chave secreta uma única vez; execute o último comando somente em uma estação protegida e armazene o resultado diretamente no segredo do OCI.

```bash
aws iam create-user --user-name "<BOOTSTRAP_USER>"

aws iam put-user-policy \
  --user-name "<BOOTSTRAP_USER>" \
  --policy-name "oracle-bootstrap-assume-migration-role" \
  --policy-document file://bootstrap-assume-role-policy.json

aws iam create-access-key \
  --user-name "<BOOTSTRAP_USER>" \
  --output json
```

## 5. Criar a função de migração

### 5.1 Política de confiança (*trust policy*)

1. Em **IAM → Roles → Create role**, crie `oracle-s3-migration-role`.
2. Selecione **Custom trust policy**.
3. Cole o documento abaixo, substituindo a conta e o usuário de inicialização.
4. Configure a duração máxima da sessão em uma hora como padrão inicial. A aplicação assume novamente a função quando necessário.

Substitua no JSON: `<AWS_ACCOUNT_ID>` e `<BOOTSTRAP_USER>`. A entidade principal (*principal*) deve ser o usuário IAM criado na seção anterior.

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

### Procedimento equivalente com a AWS CLI

Salve a política de confiança anterior como `migration-trust-policy.json` e crie a função com duração máxima de sessão de uma hora:

```bash
aws iam create-role \
  --role-name "<MIGRATION_ROLE>" \
  --assume-role-policy-document file://migration-trust-policy.json \
  --max-session-duration 3600
```

### 5.2 Política mínima da função de migração

Anexe a política em linha abaixo. Ela cobre o fluxo atualmente usado pelo trabalhador (*worker*) real:

- `ListObjectsV2` na descoberta remota e na leitura dos artefatos de controle;
- `HeadObject`, `GetObject` e *range GET* durante a consulta periódica (*polling*), transferência e retomada *multipart*;
- *tags*, somente quando a preservação de *tags* estiver habilitada na aplicação;
- criação e consulta de tarefas (*jobs*) do *S3 Batch Operations*;
- `iam:PassRole` limitado ao serviço *S3 Batch Operations*.

Substitua somente estes valores no JSON:

- `<SOURCE_BUCKET>` e `<SOURCE_PREFIX>` pelo *bucket* e prefixo que serão cadastrados como origem;
- `<CONTROL_BUCKET>` e `<CONTROL_PREFIX>` pelo *bucket* e prefixo de artefatos da conexão;
- `<AWS_ACCOUNT_ID>` e `<BATCH_ROLE>` pelo recurso correto do `iam:PassRole`.

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

### Procedimento equivalente com a AWS CLI

Salve a política anterior como `migration-policy.json` e aplique-a à função de migração:

```bash
aws iam put-role-policy \
  --role-name "<MIGRATION_ROLE>" \
  --policy-name "oracle-s3-migration-policy" \
  --policy-document file://migration-policy.json
```

Se as *tags* não forem preservadas, remova as duas ações `GetObject*Tagging`. Não inclua `s3:ListJobs`, `s3:UpdateJobPriority`, `s3:UpdateJobStatus`, `s3:*` ou `Resource: "*"` no Amazon S3 apenas para facilitar o diagnóstico: essas permissões não são necessárias ao fluxo da aplicação.

## 6. Criar a função do *S3 Batch Operations*

Esta função é necessária quando a origem contém objetos em classes de arquivamento (*archive*) que exigem restauração, como *S3 Glacier Flexible Retrieval* e *S3 Glacier Deep Archive*. Ela é assumida pelo serviço ***S3 Batch Operations***, não pelo usuário IAM da conexão nem pela máquina virtual (VM).

O fluxo é o seguinte:

1. A aplicação cria no *bucket* de controle o manifesto da onda (*wave*): a lista de objetos cuja restauração será solicitada.
2. A função de migração cria uma tarefa do *S3 Batch Operations* e fornece ao serviço AWS o ARN desta função.
3. O *S3 Batch Operations* assume esta função, lê o manifesto e executa `s3:RestoreObject` somente nos objetos autorizados.
4. Ao terminar, o serviço grava o relatório de conclusão (*completion report*) no mesmo prefixo do *bucket* de controle.
5. A aplicação consulta a tarefa e lê o relatório para registrar a evidência de aceitação, falha ou conclusão da restauração.

Portanto, esta função **não** recebe permissões gerais de leitura dos dados de origem. Ela recebe apenas permissão para restauração no prefixo aprovado, leitura do manifesto e gravação do relatório.

### 6.1 Criar a função e configurar a política de confiança

Pelo Console AWS:

1. Acesse **IAM → Roles → Create role**.
2. Em **Trusted entity type**, selecione **Custom trust policy**.
3. Substitua o conteúdo pelo JSON abaixo. Não altere o principal de serviço `batchoperations.s3.amazonaws.com`.
4. Avance sem adicionar políticas gerenciadas pela AWS.
5. Em **Role name**, informe o valor de `<BATCH_ROLE>` registrado na seção 2. Exemplo: `oracle-s3-batch-restore-role`.
6. Clique em **Create role**.
7. Abra a função criada, acesse **Trust relationships → Edit trust policy** e confirme que o documento salvo é exatamente o apresentado abaixo.

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

### Procedimento equivalente com a AWS CLI

Salve a política de confiança anterior como `batch-trust-policy.json` e execute:

```bash
aws iam create-role \
  --role-name "<BATCH_ROLE>" \
  --assume-role-policy-document file://batch-trust-policy.json
```

O principal de confiança deve ser somente `batchoperations.s3.amazonaws.com`. Não adicione usuário IAM, conta AWS, VM, `s3.amazonaws.com` ou outros serviços a esta política de confiança.

### 6.2 Adicionar a política mínima da função do *Batch*

Pelo Console AWS:

1. Dentro da função `<BATCH_ROLE>`, abra **Permissions → Add permissions → Create inline policy**.
2. Selecione a aba **JSON**.
3. Cole o JSON abaixo depois de substituir os quatro placeholders indicados.
4. Clique em **Next**.
5. Em **Policy name**, use `oracle-s3-batch-restore-policy`.
6. Verifique se os ARNs apontam somente para o *bucket*/prefixo da origem e para o *bucket*/prefixo de controle da mesma conexão.
7. Clique em **Create policy**.

Substitua no JSON: `<SOURCE_BUCKET>`, `<SOURCE_PREFIX>`, `<CONTROL_BUCKET>` e `<CONTROL_PREFIX>`. Esses valores devem ser idênticos aos usados na política da função de migração.

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

### Procedimento equivalente com a AWS CLI

Salve a política anterior como `batch-restore-policy.json` e aplique-a à função do *Batch*:

```bash
aws iam put-role-policy \
  --role-name "<BATCH_ROLE>" \
  --policy-name "oracle-s3-batch-restore-policy" \
  --policy-document file://batch-restore-policy.json
```

### 6.3 Validar a função

1. Confirme que a função de migração contém `iam:PassRole` para `arn:aws:iam::<AWS_ACCOUNT_ID>:role/<BATCH_ROLE>`.
2. Confirme que esse `iam:PassRole` possui a condição `iam:PassedToService = batchoperations.s3.amazonaws.com`.
3. Confirme que a aplicação está configurada com o ARN exato da função do *Batch* no segredo da conexão.
4. Execute a pré-validação (*pre-check*) na interface. Ela deve validar a credencial e o `AssumeRole` da função de migração.
5. Na primeira onda com objetos arquivados, acompanhe o relatório: a submissão deve registrar um identificador de tarefa (*Batch job ID*) e um relatório de conclusão no *bucket* de controle.

O relatório de conclusão integra a evidência operacional. Não use um *bucket* público para manifestos ou relatórios e não altere manualmente objetos já referenciados por uma tarefa em execução.

Com a AWS CLI, confirme a função, as respectivas políticas em linha e a política de confiança antes de submeter a primeira onda:

```bash
aws iam get-role --role-name "<MIGRATION_ROLE>"
aws iam get-role-policy \
  --role-name "<MIGRATION_ROLE>" \
  --policy-name "oracle-s3-migration-policy"

aws iam get-role --role-name "<BATCH_ROLE>"
aws iam get-role-policy \
  --role-name "<BATCH_ROLE>" \
  --policy-name "oracle-s3-batch-restore-policy"
```

## 7. Inventário do Amazon S3 (recomendado para inventários grandes)

Para origens com mais de 1 milhão de objetos, prefira o *S3 Inventory* em vez da descoberta remota por interface de programação de aplicações (*Application Programming Interface* — API). A aplicação importa arquivos CSV codificados em UTF-8, opcionalmente compactados em GZIP, e precisa somente ler o prefixo de entrega; não precisa criar nem alterar a configuração do inventário. Alternativamente, o operador pode enviar manualmente um inventário compatível pela interface.

1. No *bucket* de origem, abra **Management → Inventory configurations → Create inventory configuration**.
2. Nome sugerido: `oracle-daily-inventory`.
3. Selecione a frequência **Daily** e o formato **CSV** com compactação **GZIP**.
4. Escolha um *bucket*/prefixo de destino dedicado, por exemplo, `s3://customer-inventory-reports/oracle/customer-finance-archive/`.
5. Marque ao menos: **Object key**, **Size**, **Last modified date**, **ETag** e **Storage class**. Para *bucket* versionado, inclua também **Version ID**.
6. Conceda ao serviço Amazon S3 permissão de gravação no *bucket* de destino. Restrinja a política por `aws:SourceAccount` e `aws:SourceArn`.
7. Na função de migração, adicione somente leitura do prefixo de entrega:

Substitua `<INVENTORY_BUCKET>` e `<INVENTORY_PREFIX>` pelo *bucket* e prefixo de entrega configurados no *S3 Inventory*.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadApprovedInventoryDeliveryPrefix",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::<INVENTORY_BUCKET>/<INVENTORY_PREFIX>*"
    }
  ]
}
```

O primeiro relatório não é imediato; aguarde a primeira entrega do Amazon S3 antes de importar. Não conceda `s3:PutInventoryConfiguration` à aplicação.

### Procedimento equivalente com a AWS CLI

Crie o arquivo `inventory-configuration.json` abaixo. Se o *bucket* de entrega estiver em outra conta, substitua `<AWS_ACCOUNT_ID>` pelo identificador da conta proprietária desse *bucket*.

```json
{
  "Id": "oracle-daily-inventory",
  "IsEnabled": true,
  "IncludedObjectVersions": "Current",
  "Schedule": {
    "Frequency": "Daily"
  },
  "Destination": {
    "S3BucketDestination": {
      "AccountId": "<AWS_ACCOUNT_ID>",
      "Bucket": "arn:aws:s3:::<INVENTORY_BUCKET>",
      "Format": "CSV",
      "Prefix": "<INVENTORY_PREFIX>",
      "Encryption": {
        "SSES3": {}
      }
    }
  },
  "OptionalFields": [
    "Size",
    "LastModifiedDate",
    "ETag",
    "StorageClass",
    "VersionId"
  ]
}
```

Salve também a política abaixo como `inventory-destination-policy.json`. Caso o *bucket* de entrega já tenha uma política, incorpore esta declaração à política existente; não a substitua sem revisar as permissões já concedidas.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowS3InventoryDelivery",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::<INVENTORY_BUCKET>/<INVENTORY_PREFIX>*",
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "<AWS_ACCOUNT_ID>"
        },
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:::<SOURCE_BUCKET>"
        }
      }
    }
  ]
}
```

Em seguida, execute:

```bash
aws s3api put-bucket-policy \
  --bucket "<INVENTORY_BUCKET>" \
  --policy file://inventory-destination-policy.json

aws s3api put-bucket-inventory-configuration \
  --bucket "<SOURCE_BUCKET>" \
  --id "oracle-daily-inventory" \
  --inventory-configuration file://inventory-configuration.json

aws iam put-role-policy \
  --role-name "<MIGRATION_ROLE>" \
  --policy-name "oracle-s3-inventory-read" \
  --policy-document file://inventory-read-policy.json
```

Para o último comando, salve a política de leitura apresentada antes desta subseção como `inventory-read-policy.json`.

## 8. Acesso entre contas AWS (opcional)

Quando o *bucket* e as funções estão em contas distintas:

1. Mantenha o usuário de inicialização, a função de migração e a função do *Batch* na conta que administrará a conexão.
2. Na conta proprietária do *bucket*, crie uma política de *bucket* que permita à função de migração somente a listagem do prefixo e a leitura dos objetos; permita à função do *Batch* somente `s3:RestoreObject` no mesmo prefixo.
3. Teste primeiro um prefixo pequeno.

Não substitua os ARNs de objeto por `bucket/*` se a origem aprovada corresponder somente a um prefixo.

### Procedimento equivalente com a AWS CLI

Na conta proprietária do *bucket* de origem, salve a política abaixo como `cross-account-source-policy.json`. Caso o *bucket* já possua política, incorpore as declarações à política existente; não a substitua sem revisar as permissões já concedidas. Substitua `<ROLE_ACCOUNT_ID>` pelo identificador da conta AWS que contém as funções de migração e do *Batch*.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowMigrationRoleToListApprovedPrefix",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<ROLE_ACCOUNT_ID>:role/<MIGRATION_ROLE>"
      },
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
      "Sid": "AllowMigrationRoleToReadApprovedObjects",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<ROLE_ACCOUNT_ID>:role/<MIGRATION_ROLE>"
      },
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetObjectTagging",
        "s3:GetObjectVersionTagging"
      ],
      "Resource": "arn:aws:s3:::<SOURCE_BUCKET>/<SOURCE_PREFIX>*"
    },
    {
      "Sid": "AllowBatchRoleToRestoreApprovedObjects",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<ROLE_ACCOUNT_ID>:role/<BATCH_ROLE>"
      },
      "Action": "s3:RestoreObject",
      "Resource": "arn:aws:s3:::<SOURCE_BUCKET>/<SOURCE_PREFIX>*"
    }
  ]
}
```

Execute o comando abaixo com uma identidade autorizada a alterar a política do *bucket* de origem:

```bash
aws s3api put-bucket-policy \
  --bucket "<SOURCE_BUCKET>" \
  --policy file://cross-account-source-policy.json
```

## 9. Criar o segredo no OCI e cadastrar a conexão

Após criar os recursos AWS:

1. No cofre (*Vault*) do OCI, crie ou atualize um segredo (*Secret*) com o JSON da conexão.
2. Use `connection_name` como rótulo imutável e compreensível; a aplicação usa identificadores internos para vínculos e histórico.
3. Informe o `aws_account_id` correto. Esse valor é validado na pré-validação.
4. Use um `control_bucket` exclusivo para essa conta e essa conexão.
5. Em **Configurações → AWS connections**, execute **Refresh OCI Secrets**, escolha o segredo compatível e registre a conexão.
6. Execute a pré-validação antes de criar uma origem.

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

Nunca envie o JSON por conversa, e-mail, Git, registros (*logs*) ou navegador. A aplicação não exibe nem persiste os dois campos de credencial.

## 10. Validação operacional

Execute estes comandos a partir de uma estação autorizada, usando o perfil do usuário de inicialização:

```bash
aws sts get-caller-identity
aws sts assume-role \
  --role-arn arn:aws:iam::<AWS_ACCOUNT_ID>:role/<MIGRATION_ROLE> \
  --role-session-name oracle-precheck
```

Com as credenciais temporárias retornadas, valide somente o escopo aprovado:

```bash
aws s3api list-objects-v2 \
  --bucket "<SOURCE_BUCKET>" \
  --prefix "<SOURCE_PREFIX>" \
  --max-keys 1

aws s3api head-bucket --bucket "<CONTROL_BUCKET>"
```

A pré-validação (*pre-check*) valida a identidade AWS e `AssumeRole` sem listar, restaurar ou copiar objetos. Antes da entrada em produção, registre a aprovação das políticas, o prefixo autorizado, o *bucket* de controle e o resultado da pré-validação.

Checklist mínimo:

- [ ] Usuário de inicialização sem acesso ao console e com uma única ação `sts:AssumeRole`.
- [ ] Política de confiança da função de migração aponta somente para o usuário de inicialização correto.
- [ ] Política de confiança da função do *Batch* aponta somente para `batchoperations.s3.amazonaws.com`.
- [ ] Políticas estão limitadas a *buckets* e prefixos aprovados.
- [ ] `iam:PassRole` está condicionado a `batchoperations.s3.amazonaws.com`.
- [ ] *Bucket* de controle possui Block Public Access, *ownership enforced* e regra de ciclo de vida.
- [ ] O segredo do OCI foi registrado e a pré-validação foi aprovada.

## Referências AWS

- [Funções e políticas IAM para o *S3 Batch Operations*](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-iam-role-policies.html)
- [Criar tarefas do *S3 Batch Operations*](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-create-job.html)
- [Restaurar objetos por meio do *S3 Batch Operations*](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-initiate-restore-object.html)
- [Relatórios de conclusão do *S3 Batch Operations*](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-examples-reports.html)
- [Configurar o *S3 Inventory*](https://docs.aws.amazon.com/AmazonS3/latest/userguide/configure-inventory.html)

## Anexo A — Lista de abreviaturas, siglas e termos

| Sigla ou termo | Significado |
|---|---|
| Amazon S3 | *Amazon Simple Storage Service*, serviço de armazenamento de objetos da AWS. |
| ARN | *Amazon Resource Name*, identificador de um recurso AWS. |
| AWS CLI | *Amazon Web Services Command Line Interface*, interface de linha de comando da AWS. |
| CSV | *Comma-Separated Values*, formato tabular de valores separados por vírgula. |
| ETag | *Entity Tag*, identificador associado a uma versão de objeto. |
| GZIP | *GNU zip*, formato de compactação de arquivos. |
| IAM | *Identity and Access Management*, serviço AWS de gestão de identidades e acessos. |
| JSON | *JavaScript Object Notation*, formato textual estruturado usado em políticas e configurações. |
| OCI | *Oracle Cloud Infrastructure*. |
| PDF | *Portable Document Format*. |
| STS | *Security Token Service*, serviço AWS que emite credenciais temporárias. |
| SSE-S3 | *Server-Side Encryption with Amazon S3 Managed Keys*, criptografia no servidor com chaves gerenciadas pelo Amazon S3. |
| UTF-8 | *Unicode Transformation Format – 8 bit*, codificação de caracteres. |

Neste documento, *bucket* designa um repositório de objetos; *role*, uma função do IAM; *policy*, uma política de permissões; e *prefix*, um prefixo lógico de objetos em um *bucket*.
