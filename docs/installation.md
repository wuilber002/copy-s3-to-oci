# Installation

Este guia instala a plataforma de migração e o simulador Fujin em uma VM OCI
já preparada. A preparação de rede, VM, Vault, *Secrets*, Dynamic Group,
policies, bucket de destino e backups de boot volume é tratada exclusivamente
no [OCI Setup](oci-setup.md).

> **Pré-requisito:** conclua o OCI Setup antes de iniciar. A VM precisa ter
> acesso ao repositório por um canal corporativo aprovado. O caminho de dados
> entre OCI e AWS deve usar a conectividade dedicada previamente disponibilizada
> pelo cliente; este guia não configura roteamento ou conectividade de rede.

## 1. Informações necessárias

Registre os valores abaixo antes de acessar a VM. Na Console OCI, os recursos
são selecionados pelos respectivos nomes exibidos; os OCIDs desta tabela são
usados somente neste guia para a configuração de runtime e para comandos CLI.

| Código | Finalidade | Exemplo |
|---|---|---|
| `<LINUX_USER>` | Usuário Linux autorizado pela chave SSH da VM. | `opc` |
| `<VM_HOST>` | IP ou nome DNS administrativo da VM. | `10.0.10.25` |
| `<REPOSITORY_URL>` | URL Git corporativa ou espelho aprovado do código. | `https://github.com/wuilber002/raijin-data-migration.git` |
| `<RELEASE_REF>` | Tag ou *commit* aprovado para a instalação. | `main` |
| `<OBJECT_STORAGE_NAMESPACE>` | Namespace do Object Storage do tenancy. | `axaxaxaxax` |
| `<SECRETS_COMPARTMENT_OCID>` | Compartment que contém os Secrets da plataforma. | `ocid1.compartment.oc1..example` |
| `<DESTINATION_COMPARTMENT_OCID>` | Compartment que contém os buckets OCI autorizados. | `ocid1.compartment.oc1..example` |
| `<POSTGRES_SECRET_OCID>` | OCID do Secret `postgres_password`. | `ocid1.vaultsecret.oc1..example` |
| `<SIMULATION_POSTGRES_SECRET_OCID>` | OCID do Secret `simulation_postgres_password`. | `ocid1.vaultsecret.oc1..example` |

## 2. Acessar e preparar a VM

Na estação administrativa, conecte-se usando somente a chave SSH aprovada:

```bash
ssh <LINUX_USER>@<VM_HOST>
```

Instale Git e Podman na Oracle Linux:

```bash
sudo dnf install -y git podman
git --version
podman --version
```

Não abra a porta TCP 8080. A instalação publica a interface exclusivamente em
`127.0.0.1:8080`; o acesso administrativo será feito por túnel SSH.

## 3. Criar a configuração de runtime

O arquivo abaixo contém somente OCIDs e metadados de OCI. Ele não contém os
valores das senhas, *access keys* ou outros conteúdos dos Secrets.

```bash
sudo install -d -m 0755 /etc/s3-oci-migration
sudo vi /etc/s3-oci-migration/oci-runtime.json
```

Cole o JSON e substitua **todos** os valores entre `<...>`:

```json
{
  "object_storage_namespace": "<OBJECT_STORAGE_NAMESPACE>",
  "destination_compartment_names": {
    "<DESTINATION_COMPARTMENT_OCID>": "<DESTINATION_COMPARTMENT_NAME>"
  },
  "secret_ocids": {
    "postgres_password": "<POSTGRES_SECRET_OCID>",
    "simulation_postgres_password": "<SIMULATION_POSTGRES_SECRET_OCID>"
  },
  "secrets_compartment_ocid": "<SECRETS_COMPARTMENT_OCID>",
  "secret_compartment_ocids": [
    "<SECRETS_COMPARTMENT_OCID>"
  ]
}
```

Proteja o arquivo após salvar:

```bash
sudo chmod 0600 /etc/s3-oci-migration/oci-runtime.json
```

Para permitir leitura de Secrets em compartments adicionais, inclua cada OCID
em `secret_compartment_ocids` e confirme que as policies correspondentes foram
criadas conforme o OCI Setup.

## 4. Instalar a release e iniciar os serviços

Baixe a release aprovada para o caminho persistente esperado pelo bootstrap:

```bash
sudo install -d -m 0755 /opt/s3-oci-migration
sudo git clone --branch "<RELEASE_REF>" "<REPOSITORY_URL>" \
  /opt/s3-oci-migration/release
```

Execute o bootstrap e habilite a plataforma para as próximas inicializações:

```bash
sudo /opt/s3-oci-migration/release/scripts/bootstrap.sh
sudo systemctl enable --now s3-oci-migration.service
```

O bootstrap configura o PostgreSQL persistente no boot volume, os workers
Raikou e Raiju, a API local, o backend Fujin quando o modo de simulação estiver
ativo, os timers de backup lógico e de estado da plataforma. A primeira
execução também lê as senhas do Vault pela identidade dinâmica da VM.

## 5. Validar a instalação

Confirme o serviço, a saúde da API e os timers locais:

```bash
sudo systemctl is-active s3-oci-migration.service
curl --fail --silent http://127.0.0.1:8080/healthz
curl --fail --silent http://127.0.0.1:8080/api/runtime
sudo systemctl list-timers 's3-oci-*' --all
```

Se a API não ficar saudável, consulte o log sem exibir Secrets:

```bash
sudo journalctl -u s3-oci-migration.service -n 200 --no-pager
```

Pela console, abra **Configurações**, atualize o inventário de buckets OCI,
selecione o destino e execute o pré-check. A validação lê a versão corrente dos
Secrets e lista no máximo um objeto de cada bucket configurado; ela não altera
objetos nem lista, restaura ou baixa objetos AWS.

Se o bucket não aparecer ou o pré-check falhar, confirme nesta ordem:

1. A VM pertence à Dynamic Group indicada.
2. A policy foi criada no compartment correto e já propagou.
3. O nome do bucket no statement é idêntico ao nome real.
4. O compartment de Secrets possui `inspect secret-family` e `read secret-bundles`.
5. O arquivo `oci-runtime.json` contém OCIDs corretos e nenhum valor secreto.

## 6. Acessar a interface local

Na estação administrativa, mantenha um túnel SSH aberto:

```bash
ssh -N -L 8080:127.0.0.1:8080 <LINUX_USER>@<VM_HOST>
```

Abra `http://127.0.0.1:8080` no navegador local. Não publique esse endereço
por balanceador, regra de entrada, *reverse proxy* ou IP público.

## 7. Atualizar a instalação

Atualizações devem ser executadas em uma janela operacional, após confirmar que
não há transferências ou tarefas críticas ativas. Registre a referência usada e
faça backup lógico antes da alteração.

```bash
sudo systemctl stop s3-oci-migration.service
sudo -u root git -C /opt/s3-oci-migration/release fetch --tags origin
sudo -u root git -C /opt/s3-oci-migration/release checkout "<RELEASE_REF>"
sudo systemctl start s3-oci-migration.service
```

Após a atualização, repita as verificações da seção 5. Para recuperação de
dados ou de uma instalação interrompida, siga o [runbook de recuperação](recovery-runbook.md).

## 8. Alterar o modo de operação por API local

A troca entre o runtime que acessa AWS/OCI e o runtime isolado do Fujin não é
exposta na interface web. Execute-a apenas na própria VM, após esvaziar ou
pausar as tarefas pertinentes.

Consulte o modo atual:

```bash
curl --fail --silent http://127.0.0.1:8080/api/runtime
```

Solicite o modo que acessa AWS e OCI reais:

```bash
curl --fail --silent --show-error \
  --request POST http://127.0.0.1:8080/api/runtime/mode \
  --header 'Content-Type: application/json' \
  --data '{"target_mode":"REAL","confirmed":true}'
```

Solicite o modo isolado, sem chamadas a AWS ou OCI:

```bash
curl --fail --silent --show-error \
  --request POST http://127.0.0.1:8080/api/runtime/mode \
  --header 'Content-Type: application/json' \
  --data '{"target_mode":"SIMULATION","confirmed":true}'
```

Uma resposta `202` confirma que o host aceitou a solicitação. Aguarde o
reinício e confirme o novo modo:

```bash
until curl --fail --silent http://127.0.0.1:8080/healthz; do sleep 5; done
curl --fail --silent http://127.0.0.1:8080/api/runtime
```
