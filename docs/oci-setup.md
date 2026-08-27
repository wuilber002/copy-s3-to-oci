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
| Conectividade dedicada OCI–AWS | Leva todo o tráfego de Amazon S3 e AWS STS pelo caminho privado aprovado, via FastConnect/DRG e a conectividade AWS correspondente. |
| Boot volume persistente | Mantém banco, checkpoints multipart, evidências, *logs* e backups lógicos após reinicialização da VM. |
| Bucket OCI de destino | Recebe os objetos migrados. A autorização é limitada por nome de *bucket*. |
| Vault e chave | Protegem as senhas do PostgreSQL e os JSONs das conexões AWS. |
| Dynamic Group | Representa exclusivamente a VM perante os serviços OCI. |
| Policy OCI | Permite à Dynamic Group ler os *Secrets* e manipular objetos apenas nos destinos aprovados. |

A interface é publicada somente em `127.0.0.1:8080` dentro da VM. O acesso
administrativo ocorre por túnel SSH autenticado por chave pública/privada.

## 2. Informações a registrar antes do início

Registre os valores a seguir. Os mesmos códigos serão reutilizados nos comandos
e nas policies. A tabela é uma referência, não um comando.

| Código | O que informar | Exemplo |
|---|---|---|
| `<TENANCY_OCID>` | OCID do *tenancy* OCI. | `ocid1.tenancy.oc1..example` |
| `<VM_COMPARTMENT_OCID>` | Compartment onde a VM será criada. | `ocid1.compartment.oc1..example` |
| `<SUBNET_OCID>` | Subnet existente, conectada ao DRG, com rota privada para AWS e acesso aos serviços OCI. | `ocid1.subnet.oc1.sa-saopaulo-1.example` |
| `<DRG_OCID>` | Dynamic Routing Gateway (DRG) conectado à VCN e ao circuito privado. | `ocid1.drg.oc1.sa-saopaulo-1.example` |
| `<AWS_REGION>` | Região AWS das origens S3. | `us-east-1` |
| `<AWS_S3_ENDPOINT>` | Nome DNS AWS que será resolvido e alcançado pelo enlace dedicado. | `s3.us-east-1.amazonaws.com` |
| `<AVAILABILITY_DOMAIN>` | Availability Domain (AD) escolhido para a VM. | `xYzA:SA-SAOPAULO-1-AD-1` |
| `<IMAGE_OCID>` | Imagem Oracle Linux compatível. | `ocid1.image.oc1.sa-saopaulo-1.example` |
| `<DESTINATION_COMPARTMENT_OCID>` | Compartment que contém o *bucket* OCI de destino. | `ocid1.compartment.oc1..example` |
| `<DESTINATION_BUCKET>` | Nome do *bucket* OCI que receberá dados. | `migration-destination` |
| `<SECRETS_COMPARTMENT_OCID>` | Compartment em que os *Secrets* poderão ser lidos pela VM. | `ocid1.compartment.oc1..example` |
| `<VAULT_OCID>` | Vault que armazenará os *Secrets*. | `ocid1.vault.oc1.sa-saopaulo-1.example` |
| `<KEY_OCID>` | Chave AES-256 usada para cifrar os *Secrets*. | `ocid1.key.oc1.sa-saopaulo-1.example` |
| `<POLICY_COMPARTMENT_OCID>` | Compartment que governa os compartments informados na policy. | `ocid1.compartment.oc1..example` |
| `<VM_OCID>` | OCID da VM criada neste procedimento. | `ocid1.instance.oc1.sa-saopaulo-1.example` |
| `<DYNAMIC_GROUP_NAME>` | Nome da Dynamic Group exclusiva da VM. | `oracle-data-migration-vm` |

Para listar recursos pela OCI CLI, configure previamente o perfil da sua conta
administrativa e execute, por exemplo:

```bash
oci iam availability-domain list \
  --compartment-id "<TENANCY_OCID>" \
  --query 'data[].name' --output table

oci os ns get --query data --raw-output
```

## 3. Preparar rede dedicada OCI–AWS e acesso administrativo

Neste cenário, a VM **não usa a internet para acessar AWS**. Todo tráfego de
Amazon S3, AWS Security Token Service (STS) e *S3 Batch Operations* deve
seguir pelo enlace dedicado contratado: FastConnect conectado ao DRG da VCN e
ao componente AWS correspondente. A rota de retorno AWS para a VCN também é
obrigatória.

FastConnect é a entrada privada da VCN pelo DRG; ele não cria, por si só, um
endpoint Amazon S3. O time de redes deve homologar uma das arquiteturas abaixo:

1. **Endpoint privado S3 na AWS (recomendado):** a VCN chega, por caminho
   dedicado, a uma VPC AWS com *Interface VPC Endpoint* para Amazon S3. O DNS
   corporativo/Route 53 deve resolver os nomes S3 usados pela região para esse
   endpoint privado. Esse modelo evita depender de endereços públicos do S3.
2. **Direct Connect public virtual interface (VIF):** o enlace dedicado anuncia
   e alcança os prefixos públicos AWS necessários para `s3.<região>.amazonaws.com`
   e STS. O nome continua público, porém o caminho de rede até a AWS usa a VIF
   pública, não um Internet Gateway da VCN.

Quando OCI e AWS estiverem em regiões compatíveis, o Oracle Interconnect for
AWS pode fornecer a interconexão gerenciada entre FastConnect/DRG e AWS Direct
Connect Gateway. Em outras combinações de regiões, o cliente deve fornecer a
interconexão por parceiro ou rede própria. A disponibilidade e a arquitetura
dependem da região; valide o desenho com as equipes OCI e AWS antes da criação
da VM. A documentação oficial descreve o papel do DRG, o circuito de
interconexão e as regiões suportadas pelo [Oracle Interconnect for AWS](https://docs.oracle.com/en-us/iaas/Content/multicloud/interconnect-aws.htm).

### 3.1 Requisitos de rota, DNS e firewall

Antes de criar a VM, o time de rede deve comprovar todos os itens:

| Item | Requisito |
|---|---|
| VCN e DRG | A VCN da `<SUBNET_OCID>` está anexada ao `<DRG_OCID>`. |
| Rota de saída | A tabela de rota da subnet envia os CIDRs/endpoints AWS aprovados para o DRG. |
| Rota de retorno | AWS anuncia ou possui rota estática de retorno para o CIDR da VCN. |
| DNS | `<AWS_S3_ENDPOINT>` e o endpoint STS regional resolvem para endereços alcançáveis pelo enlace dedicado. |
| Firewall | A VM pode abrir TCP 443 apenas para os endpoints AWS privados/aprovados e os serviços OCI necessários. |
| Repositório | A instalação e atualização podem usar um proxy corporativo ou canal GitHub aprovado; isso é separado do caminho AWS. |

Na *security list* ou no Network Security Group (NSG), permita entrada SSH
(TCP 22) somente conforme a política corporativa. Não é necessário abrir TCP
8080: a aplicação aceita conexões somente no *loopback* da VM. A proteção de
acesso é a chave SSH; senha, teclado interativo e login de `root` são
desabilitados no bootstrap.

Na Console OCI:

1. Abra **Networking → Dynamic Routing Gateways** e confirme o attachment da
   VCN ao `<DRG_OCID>`.
2. Na tabela de rotas da `<SUBNET_OCID>`, confirme os destinos AWS apontando
   para o DRG, sem rota padrão para Internet Gateway para o tráfego AWS.
3. Confirme com a equipe AWS a rota de retorno e o anúncio BGP correspondente.
4. Valide a resolução DNS de S3 e STS pelo resolvedor usado pela VM.
5. Inclua a regra SSH aprovada pelo cliente e não crie regra de entrada para
   TCP 8080.

O cliente pode validar DNS e conectividade TCP a partir de uma VM de teste na
mesma subnet antes de instalar a plataforma:

```bash
getent hosts "s3.<AWS_REGION>.amazonaws.com"
getent hosts "sts.<AWS_REGION>.amazonaws.com"

timeout 10 bash -c '</dev/tcp/s3.<AWS_REGION>.amazonaws.com/443>'
timeout 10 bash -c '</dev/tcp/sts.<AWS_REGION>.amazonaws.com/443>'
```

Os comandos acima validam resolução e abertura TCP; eles não autenticam, não
listam objetos e não iniciam uma migração. A equipe de redes deve confirmar por
telemetria do DRG/VIF que o tráfego percorreu o circuito dedicado.

## 4. Criar o bucket OCI de destino

O destino pode existir previamente. Se criar um novo, use um *bucket* privado
no compartment de destino e registre nome e compartment. A aplicação preserva
a chave do objeto e grava metadados de migração; não requer acesso a outros
*buckets*.

Na Console OCI:

1. Abra **Object Storage & Archive Storage → Buckets**.
2. Selecione o compartment `<DESTINATION_COMPARTMENT_OCID>`.
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
mesmo compartment de segurança e registre os OCIDs retornados.

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
2. Selecione `<KEY_OCID>`.
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
2. Selecione `<VM_COMPARTMENT_OCID>`, a imagem Oracle Linux aprovada e
   `<AVAILABILITY_DOMAIN>`.
3. Escolha `VM.Standard.E5.Flex` e configure a capacidade desejada.
4. Em **Networking**, selecione `<SUBNET_OCID>`. Atribua IP público somente se
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

## 10. Instalar a plataforma na VM

Conecte-se à VM por SSH e execute o procedimento abaixo como usuário com `sudo`.
O repositório deve ser obtido por meio do canal GitHub aprovado pelo cliente.
O bootstrap instala e inicia PostgreSQL, interface, Raikou e Raiju como
containers Podman, registra o serviço `s3-oci-migration.service`, configura
SSH somente por chave e cria os timers de backup e de estado.

Crie primeiro o arquivo de runtime. Ele contém apenas OCIDs e nomes, nunca os
valores dos *Secrets*. Substitua todos os valores entre `<...>`:

```json
{
  "object_storage_namespace": "<OBJECT_STORAGE_NAMESPACE>",
  "destination_compartment_names": {
    "<DESTINATION_COMPARTMENT_OCID>": "<DESTINATION_COMPARTMENT_NAME>"
  },
  "secret_ocids": {
    "postgres_password": "<POSTGRES_PASSWORD_SECRET_OCID>",
    "simulation_postgres_password": "<SIMULATION_POSTGRES_PASSWORD_SECRET_OCID>"
  },
  "secrets_compartment_ocid": "<SECRETS_COMPARTMENT_OCID>",
  "secret_compartment_ocids": [
    "<SECRETS_COMPARTMENT_OCID>"
  ]
}
```

Na VM:

```bash
sudo dnf install -y git podman
sudo install -d -m 0755 /opt/s3-oci-migration
sudo git clone --depth 1 https://github.com/wuilber002/raijin-data-migration.git \
  /opt/s3-oci-migration/release

sudo install -d -m 0755 /etc/s3-oci-migration
sudo install -m 0600 /dev/null /etc/s3-oci-migration/oci-runtime.json
sudo vi /etc/s3-oci-migration/oci-runtime.json

sudo /opt/s3-oci-migration/release/scripts/bootstrap.sh
sudo systemctl enable --now s3-oci-migration.service
sudo systemctl status s3-oci-migration.service --no-pager
```

Use uma *release* ou referência Git aprovada para instalações produtivas, em
vez de acompanhar automaticamente alterações de uma ramificação. O bootstrap
materializa as senhas em arquivos locais de permissão restrita para permitir
reinício seguro; o Vault permanece a fonte de verdade.

## 11. Configurar backups persistentes

O bootstrap cria backup lógico diário do PostgreSQL às 02:15 UTC e mantém os
dados no boot volume. Crie também uma policy de backup do boot volume, diária,
com retenção inicial de 35 dias. Isso protege a VM e o disco contra exclusão ou
falha acidental; o backup lógico acelera uma recuperação seletiva do banco.

Na Console OCI:

1. Abra **Block Storage → Boot Volumes** e selecione o volume da VM.
2. Em **Backup Policies**, associe ou crie uma política diária.
3. Configure retenção de 35 dias como ponto de partida.
4. Confirme que há pelo menos um backup bem-sucedido antes de iniciar a
   migração de produção.

## 12. Acessar a interface por túnel SSH

Na estação administrativa, execute:

```bash
ssh -N -L 8080:127.0.0.1:8080 <USUARIO_LINUX>@<IP_OU_HOSTNAME_DA_VM>
```

Abra `http://127.0.0.1:8080` no navegador local. Não publique esse endereço
por balanceador, regra de entrada, *reverse proxy* ou IP público.

## 13. Validar o ambiente antes da primeira origem

Na VM, confira serviço e API:

```bash
sudo systemctl is-active s3-oci-migration.service
curl --fail --silent http://127.0.0.1:8080/healthz
curl --fail --silent http://127.0.0.1:8080/api/runtime
```

Pela interface, abra **Configurações**, atualize o inventário de buckets OCI,
selecione o destino e execute o pré-check. A validação lê a versão corrente
dos *Secrets* e lista no máximo um objeto de cada bucket configurado; ela não
altera objetos nem chama a AWS.

Se o *bucket* não aparecer ou o pré-check falhar, confirme nesta ordem:

1. A VM pertence à Dynamic Group indicada.
2. A policy foi criada no compartment correto e já propagou.
3. O nome do bucket no statement é idêntico ao nome real.
4. O compartment do *Secret* possui `inspect secret-family` e
   `read secret-bundles`.
5. O arquivo `/etc/s3-oci-migration/oci-runtime.json` tem os OCIDs corretos e
   nenhuma senha.

## 14. Incluir novos destinos ou novos compartments de Secrets

Para um novo bucket no mesmo compartment, inclua seu nome na condição `any`
do statement `manage objects`, aguarde a propagação e atualize o inventário de
buckets OCI na interface.

Para um novo compartment de destino, acrescente os dois statements desse
compartment (`inspect buckets` e `manage objects`). Para um novo compartment de
*Secrets*, acrescente os dois statements de leitura e inclua seu OCID em
`secret_compartment_ocids` no arquivo de runtime; em seguida, reinicie o
serviço de forma controlada:

```bash
sudo systemctl restart s3-oci-migration.service
```

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
