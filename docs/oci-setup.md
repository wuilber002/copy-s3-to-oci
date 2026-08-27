# OCI Setup

Este guia descreve o preparo manual, pela Console da Oracle Cloud Infrastructure
(OCI) ou pela OCI Command Line Interface (OCI CLI), do ambiente que executa a
plataforma de migração. Ele não depende de automação de infraestrutura.

O objetivo é criar uma única máquina virtual (VM) com estado persistente,
acesso restrito à interface local, leitura de *Secrets* no Vault e escrita
somente nos *buckets* OCI explicitamente autorizados.

> **Substituições obrigatórias:** todo valor entre `<...>` deve ser substituído
> antes da execução. Não use permissões `in tenancy` para a VM, não exponha a
> porta `8080` e não registre valores de *Secrets* em arquivos de comando,
> histórico do terminal ou documentação.

## 1. Arquitetura resultante

| Componente | Finalidade |
|---|---|
| VM Linux | Executa a interface web, PostgreSQL, Raikou (governança) e Raiju (transferência). |
| Conectividade dedicada OCI–AWS | Pré-requisito do cliente para levar o tráfego de Amazon S3 e AWS STS pelo caminho privado aprovado. |
| Boot volume persistente | Mantém banco, checkpoints multipart, evidências, *logs* e backups lógicos após reinicialização da VM. |
| Bucket OCI de destino | Recebe os objetos migrados. A autorização é limitada por nome de *bucket*. |
| Vault e chave | Protegem as senhas do PostgreSQL e os JSONs das conexões AWS. |
| Dynamic Group | Representa exclusivamente a VM perante os serviços OCI. |
| Policy OCI | Permite à Dynamic Group ler os *Secrets* e manipular objetos apenas nos destinos aprovados. |

A interface é publicada somente em `127.0.0.1:8080` dentro da VM. O acesso
administrativo ocorre por túnel SSH autenticado por chave pública/privada.

## 2. Informações a registrar antes do início

Na Console OCI, recursos são localizados e selecionados pelo **nome exibido**:
compartment, subnet, imagem, Vault, chave e VM. Nenhum OCID é informado nos
seletores visuais. Todo valor que contém `OCID` nesta tabela é usado somente
nos comandos da OCI CLI; a Console permanece orientada a nomes exibidos.

Registre os identificadores abaixo antes de executar os comandos de CLI.

| Recurso | Código usado na CLI | Exemplo | Uso |
|---|---|---|---|
| Tenancy | `<TENANCY_OCID>` | `ocid1.tenancy.oc1..example` | CLI |
| Compartment da VM | `<VM_COMPARTMENT_OCID>` | `ocid1.compartment.oc1..example` | CLI |
| Subnet | `<SUBNET_OCID>` | `ocid1.subnet.oc1.sa-saopaulo-1.example` | CLI |
| Availability Domain (AD) | `<AVAILABILITY_DOMAIN>` | `xYzA:SA-SAOPAULO-1-AD-1` | CLI |
| Imagem Oracle Linux | `<IMAGE_OCID>` | `ocid1.image.oc1.sa-saopaulo-1.example` | CLI |
| Compartment do destino | `<DESTINATION_COMPARTMENT_OCID>` | `ocid1.compartment.oc1..example` | CLI |
| Bucket de destino | `<DESTINATION_BUCKET>` | `migration-destination` | CLI |
| Compartment de Secrets | `<SECRETS_COMPARTMENT_OCID>` | `ocid1.compartment.oc1..example` | CLI |
| Vault | `<VAULT_OCID>` | `ocid1.vault.oc1.sa-saopaulo-1.example` | CLI |
| Chave mestra | `<KEY_OCID>` | `ocid1.key.oc1.sa-saopaulo-1.example` | CLI |
| Compartment da policy | `<POLICY_COMPARTMENT_OCID>` | `ocid1.compartment.oc1..example` | CLI |
| VM | `<VM_OCID>` | `ocid1.instance.oc1.sa-saopaulo-1.example` | CLI |
| Dynamic Group | `<DYNAMIC_GROUP_NAME>` | `oracle-data-migration-vm` | CLI |

Para listar recursos pela OCI CLI, configure previamente o perfil da sua conta
administrativa e execute, por exemplo:

```bash
oci iam availability-domain list \
  --compartment-id "<TENANCY_OCID>" \
  --query 'data[].name' --output table

oci os ns get --query data --raw-output
```

Para consultar o OCID de um recurso selecionado por nome, use a OCI CLI ou a
API administrativa. Por exemplo, para uma subnet:

```bash
oci network subnet list \
  --compartment-id "<VM_COMPARTMENT_OCID>" \
  --all \
  --query 'data[?"display-name"==`<SUBNET_NAME>`].id | [0]' \
  --raw-output
```

## 3. Pré-requisitos de rede e acesso administrativo

Neste cenário, a VM **não usa a internet para acessar AWS**. O cliente deve
garantir previamente que todo tráfego entre a VM e Amazon S3, AWS Security
Token Service (STS) e *S3 Batch Operations* percorra a conectividade dedicada
OCI–AWS aprovada, incluindo resolução de nomes, rotas de ida/retorno e regras
de firewall necessárias. O desenho, a implantação e a validação desse enlace
não fazem parte deste procedimento.

O acesso ao repositório de instalação e às atualizações pode usar o canal
corporativo aprovado pelo cliente, como proxy ou espelho Git; ele é separado do
caminho dedicado AWS.

Na *security list* ou no Network Security Group (NSG), permita entrada SSH
(TCP 22) somente conforme a política corporativa. Não é necessário abrir TCP
8080: a aplicação aceita conexões somente no *loopback* da VM. A proteção de
acesso é a chave SSH; senha, teclado interativo e login de `root` são
desabilitados no bootstrap.

Na Console OCI:

1. Selecione uma subnet que já atenda ao pré-requisito de conectividade
   dedicada OCI–AWS.
2. Inclua a regra SSH aprovada pelo cliente.
3. Não crie regra de entrada para TCP 8080.

## 4. Criar o bucket OCI de destino

O destino pode existir previamente. Se criar um novo, use um *bucket* privado
no compartment de destino e registre nome e compartment. A aplicação preserva
a chave do objeto e grava metadados de migração; não requer acesso a outros
*buckets*.

Na Console OCI:

1. Abra **Object Storage & Archive Storage → Buckets**.
2. Selecione o compartment de destino pelo nome exibido.
3. Clique em **Create Bucket**.
4. Informe `<DESTINATION_BUCKET>`, mantenha a visibilidade privada e confirme.
5. Anote o nome exato e o compartment.

Procedimento equivalente com OCI CLI:

```bash
export OBJECT_STORAGE_NAMESPACE="$(oci os ns get --query data --raw-output)"

oci os bucket create \
  --namespace "$OBJECT_STORAGE_NAMESPACE" \
  --compartment-id "<DESTINATION_COMPARTMENT_OCID>" \
  --name "<DESTINATION_BUCKET>"
```

Para mais de um destino, repita esta seção. Eles poderão ser agrupados na mesma
policy quando pertencem ao mesmo compartment, reduzindo o número de statements.

## 5. Criar ou selecionar Vault e chave

É possível usar um Vault e uma chave AES existentes, desde que sejam acessíveis
ao administrador que cria os *Secrets*. Caso sejam criados agora, prefira o
mesmo compartment de segurança e registre os nomes exibidos e os OCIDs usados
posteriormente pela CLI e pelo runtime.

Na Console OCI:

1. Abra **Identity & Security → Vault** e selecione o compartment desejado.
2. Clique em **Create Vault**, use um nome como `oracle-data-migration-vault`
   e o tipo padrão.
3. Aguarde o estado **Active**.
4. Abra o Vault, entre em **Master Encryption Keys** e crie uma chave AES de
   256 bits, por exemplo `oracle-data-migration-key`.
5. Registre os OCIDs do Vault e da chave.

Procedimento equivalente com OCI CLI:

```bash
oci kms management vault create \
  --compartment-id "<SECRETS_COMPARTMENT_OCID>" \
  --display-name "oracle-data-migration-vault" \
  --vault-type DEFAULT

# Aguarde o estado ACTIVE e substitua <VAULT_OCID>.
export VAULT_MANAGEMENT_ENDPOINT="$(oci kms management vault get \
  --vault-id "<VAULT_OCID>" \
  --query 'data."management-endpoint"' --raw-output)"

oci kms management key create \
  --compartment-id "<SECRETS_COMPARTMENT_OCID>" \
  --display-name "oracle-data-migration-key" \
  --key-shape '{"algorithm":"AES","length":32}' \
  --endpoint "$VAULT_MANAGEMENT_ENDPOINT"
```

## 6. Criar os Secrets de plataforma

A VM precisa de dois *Secrets* de senha distintos: um para o banco real e
outro para o banco isolado do simulador. Ambos devem conter texto UTF-8 não
vazio, com senha aleatória de ao menos 48 caracteres. Não reutilize a senha em
outros sistemas.

Crie os dois *Secrets* no Vault selecionado:

| Chave no runtime | Nome recomendado do Secret | Conteúdo |
|---|---|---|
| `postgres_password` | `oracle-data-migration-postgres-password` | Senha aleatória do PostgreSQL operacional. |
| `simulation_postgres_password` | `oracle-data-migration-simulation-postgres-password` | Senha aleatória distinta do PostgreSQL do simulador. |

Na Console OCI, para cada entrada:

1. Abra **Vault → <vault> → Secrets → Create Secret**.
2. Selecione a chave pelo nome exibido.
3. Informe o nome recomendado, uma descrição sem a senha e cole o valor no
   conteúdo da versão **Current**.
4. Salve e registre o OCID do Secret.

Procedimento equivalente com OCI CLI. Crie os arquivos de senha em uma estação
protegida, com permissão restrita, e não os envie ao repositório:

```bash
umask 077
openssl rand -base64 72 | tr -dc 'A-Za-z0-9' | head -c 48 > postgres_password.txt
openssl rand -base64 72 | tr -dc 'A-Za-z0-9' | head -c 48 > simulation_postgres_password.txt

oci vault secret create-base64 \
  --compartment-id "<SECRETS_COMPARTMENT_OCID>" \
  --vault-id "<VAULT_OCID>" \
  --key-id "<KEY_OCID>" \
  --secret-name "oracle-data-migration-postgres-password" \
  --description "Senha gerada para PostgreSQL operacional da plataforma." \
  --secret-content-content "$(base64 --wrap=0 postgres_password.txt)"

oci vault secret create-base64 \
  --compartment-id "<SECRETS_COMPARTMENT_OCID>" \
  --vault-id "<VAULT_OCID>" \
  --key-id "<KEY_OCID>" \
  --secret-name "oracle-data-migration-simulation-postgres-password" \
  --description "Senha gerada para PostgreSQL isolado do simulador." \
  --secret-content-content "$(base64 --wrap=0 simulation_postgres_password.txt)"

shred -u postgres_password.txt simulation_postgres_password.txt
```

As conexões AWS também são armazenadas como *Secrets* OCI, um JSON por conexão.
Elas podem ficar neste ou em outro compartment autorizado. Use o formato e o
procedimento de [AWS connections](aws-connections.md); a aplicação lista apenas
*Secrets* aos quais a Dynamic Group tem acesso e cujo JSON é compatível.

## 7. Criar a VM

Para um link de aproximadamente 1,2 Gbps, o ponto de partida recomendado é
`VM.Standard.E5.Flex`, 8 OCPUs, 32 GB de memória e boot volume de 500 GB. O
volume guarda PostgreSQL, backups lógicos, checkpoints e releases. Para uma
validação pequena, use no mínimo 2 OCPUs, 8 GB e 200 GB de volume.

Na Console OCI:

1. Abra **Compute → Instances → Create instance**.
2. Selecione pelo nome exibido o compartment da VM, a imagem Oracle Linux
   aprovada e o Availability Domain (AD).
3. Escolha `VM.Standard.E5.Flex` e configure a capacidade desejada.
4. Em **Networking**, selecione a subnet pelo nome exibido. Atribua IP público somente se
   ele for necessário para o túnel SSH e a regra corporativa o permitir.
5. Em **Add SSH keys**, cole a chave pública administrativa.
6. Defina o boot volume com pelo menos 200 GB; use 500 GB como referência
   inicial de produção.
7. Crie a instância, aguarde o estado **Running** e registre `<VM_OCID>`.

Procedimento equivalente com OCI CLI. O arquivo `metadata.json` contém apenas
a chave pública SSH:

```json
{
  "ssh_authorized_keys": "<SSH_PUBLIC_KEY>"
}
```

```bash
oci compute instance launch \
  --compartment-id "<VM_COMPARTMENT_OCID>" \
  --availability-domain "<AVAILABILITY_DOMAIN>" \
  --display-name "oracle-data-migration-vm" \
  --shape "VM.Standard.E5.Flex" \
  --shape-config '{"ocpus":8,"memoryInGBs":32}' \
  --subnet-id "<SUBNET_OCID>" \
  --image-id "<IMAGE_OCID>" \
  --source-boot-volume-size-in-gbs 500 \
  --assign-public-ip true \
  --metadata file://metadata.json
```

> Se a VM não possuir IP público, execute os passos administrativos por um
> bastion ou caminho privado aprovado. A interface continua exclusivamente
> local, independentemente do tipo de endereçamento da VM.

## 8. Criar a Dynamic Group da VM

A Dynamic Group não deve usar regra por compartment, *tag* ou padrão amplo. Ela
deve conter apenas a instância criada neste procedimento.

Na Console OCI:

1. Abra **Identity & Security → Domains → Default domain → Dynamic groups**.
2. Clique em **Create dynamic group**.
3. Informe `<DYNAMIC_GROUP_NAME>`.
4. Em **Rule 1**, use a regra abaixo, substituindo `<VM_OCID>`.
5. Salve e aguarde a propagação da política de identidade.

```text
ALL {instance.id = '<VM_OCID>'}
```

Procedimento equivalente com OCI CLI:

```bash
oci iam dynamic-group create \
  --compartment-id "<TENANCY_OCID>" \
  --name "<DYNAMIC_GROUP_NAME>" \
  --description "Dynamic Group exclusiva da VM de migração." \
  --matching-rule "ALL {instance.id = '<VM_OCID>'}"
```

## 9. Criar a policy mínima da VM

Crie a policy no `<POLICY_COMPARTMENT_OCID>`, que deve ser o compartment raiz
ou um ancestral comum dos compartments de *Secrets* e de destino mencionados.
A policy abaixo não contém statements `in tenancy`.

Substitua no arquivo somente:

- `<DYNAMIC_GROUP_NAME>` pelo nome da seção anterior;
- `<SECRETS_COMPARTMENT_OCID>` por cada compartment que contém *Secrets*
  acessíveis à plataforma;
- `<DESTINATION_COMPARTMENT_OCID>` pelo compartment do destino;
- `<DESTINATION_BUCKET>` pelo nome de cada bucket autorizado.

Para um compartment de *Secrets* e um bucket de destino:

```json
[
  "Allow dynamic-group <DYNAMIC_GROUP_NAME> to inspect secret-family in compartment id <SECRETS_COMPARTMENT_OCID>",
  "Allow dynamic-group <DYNAMIC_GROUP_NAME> to read secret-bundles in compartment id <SECRETS_COMPARTMENT_OCID>",
  "Allow dynamic-group <DYNAMIC_GROUP_NAME> to inspect buckets in compartment id <DESTINATION_COMPARTMENT_OCID>",
  "Allow dynamic-group <DYNAMIC_GROUP_NAME> to manage objects in compartment id <DESTINATION_COMPARTMENT_OCID> where target.bucket.name='<DESTINATION_BUCKET>'"
]
```

Para vários buckets no mesmo compartment, consolide-os no mesmo statement:

```text
Allow dynamic-group <DYNAMIC_GROUP_NAME> to manage objects in compartment id <DESTINATION_COMPARTMENT_OCID> where any {target.bucket.name='destination-finance', target.bucket.name='destination-legal'}
```

Na Console OCI:

1. Abra **Identity & Security → Policies** e selecione
   `<POLICY_COMPARTMENT_OCID>`.
2. Clique em **Create policy**.
3. Informe, por exemplo, `oracle-data-migration-vm-policy`.
4. Cole os statements no editor, após substituir todos os valores entre
   `<...>`.
5. Crie a policy e aguarde a propagação, normalmente alguns segundos.

Procedimento equivalente com OCI CLI. Salve o JSON anterior como
`oracle-data-migration-vm-policy.json`:

```bash
oci iam policy create \
  --compartment-id "<POLICY_COMPARTMENT_OCID>" \
  --name "oracle-data-migration-vm-policy" \
  --description "Leitura de Secrets e escrita restrita nos destinos aprovados." \
  --statements file://oracle-data-migration-vm-policy.json
```

Cada novo compartment de *Secrets* acrescenta dois statements. Cada novo
compartment de destinos acrescenta um `inspect buckets` e um `manage objects`;
novos buckets no mesmo compartment podem ser adicionados ao mesmo statement.
Essa consolidação reduz o consumo de statements na ramificação de policies do
cliente.

## 10. Configurar backups persistentes

Crie ou associe uma policy de backup do boot volume, diária, com retenção
inicial de 35 dias. Isso protege a VM e o disco contra exclusão ou falha
acidental. Os backups lógicos e sua configuração fazem parte da instalação da
plataforma e estão no [guia de instalação](installation.md).

Na Console OCI:

1. Abra **Block Storage → Boot Volumes** e selecione o volume da VM.
2. Em **Backup Policies**, associe ou crie uma política diária.
3. Configure retenção de 35 dias como ponto de partida.
4. Confirme que há pelo menos um backup bem-sucedido antes de iniciar a
   migração de produção.

## 11. Incluir novos destinos ou novos compartments de Secrets

Para um novo bucket no mesmo compartment, inclua seu nome na condição `any`
do statement `manage objects`, aguarde a propagação e atualize o inventário de
buckets OCI na interface.

Para um novo compartment de destino, acrescente os dois statements desse
compartment (`inspect buckets` e `manage objects`). Para um novo compartment de
*Secrets*, acrescente os dois statements de leitura. A atualização da
configuração de runtime e o reinício controlado do serviço são descritos no
[guia de instalação](installation.md).

## Anexo A — Lista de abreviaturas, siglas e termos

| Termo | Significado |
|---|---|
| AD | *Availability Domain*, domínio de disponibilidade da OCI. |
| Dynamic Group | Grupo de identidade OCI definido por regra; neste guia, representa somente a VM. |
| NSG | *Network Security Group*, conjunto de regras de rede aplicado a VNICs. |
| OCID | *Oracle Cloud Identifier*, identificador único de recurso OCI. |
| OCI CLI | Interface de linha de comando da Oracle Cloud Infrastructure. |
| Podman | Motor de containers usado pela VM para executar os componentes locais. |
| Secret | Recurso do OCI Vault que armazena conteúdo cifrado e versionado. |
| VM | Máquina virtual que hospeda a plataforma. |
| Vault | Serviço OCI que protege chaves e Secrets. |
