# RAIJIN — S3 Glacier to OCI Object Storage Migration

<p align="center">
  <img src="images/raijin-oracle-about.png" width="330" alt="RAIJIN — migração de dados AWS S3 para OCI Object Storage">
</p>

<p align="center">
  <a href="https://github.com/wuilber002/raijin-data-migration/actions/workflows/validate.yml"><img src="https://github.com/wuilber002/raijin-data-migration/actions/workflows/validate.yml/badge.svg?branch=main" alt="Validação"></a>
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white" alt="Python 3.12">
  <img src="https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Terraform-1.14-7B42BC?logo=terraform&logoColor=white" alt="Terraform">
  <img src="https://img.shields.io/badge/AWS%20S3-Glacier%20%2F%20Deep%20Archive-FF9900?logo=amazons3&logoColor=white" alt="AWS S3 Glacier e Deep Archive">
  <img src="https://img.shields.io/badge/OCI-Object%20Storage-F80000?logo=oracle&logoColor=white" alt="OCI Object Storage">
</p>

**RAIJIN** (雷神, o deus japonês do trovão) é uma plataforma de migração controlada de objetos AWS S3, incluindo Glacier Deep Archive, para OCI Object Storage. Ela opera em uma única VM OCI e mantém inventário, ondas, estado de restore, transferências e evidências de integridade em PostgreSQL.

## Estado atual

O repositório contém a infraestrutura para OCI Resource Manager e uma console web local para operar o plano de migração. A console permite cadastrar fontes, consultar inventário paginado, criar uma onda manual, gerar ondas automaticamente por tamanho ou por prefixo S3, baixar manifestos CSV para S3 Batch Operations, exportar relatórios, pausar/retomar/reprocessar/excluir ondas não executadas e acompanhar a fila durável, a saúde local, o estado dos serviços da VM, a prontidão OCI e o histórico operacional.

O procedimento de recursos AWS por AWS CloudShell/CLI está em [docs/aws-cli-setup.md](docs/aws-cli-setup.md). Ele é documentação; não há scripts de provisionamento AWS publicados no repositório.

A console separa o trabalho em **Status** (alertas, fila, transferências e auditorias em andamento), **Migrações** (fontes, inventário e ondas) e **Configurações** (ícone de engrenagem; parâmetros globais e pré-check OCI).

O seletor **Idioma / Language** no cabeçalho alterna a interface entre português e inglês. Estados operacionais e termos técnicos, como `PLANNED`, `READY FOR RESTORE`, `TRANSFERRING`, `COMPLETED`, `Discovery`, `Restore` e `SHA-256`, permanecem em inglês nos dois idiomas para preservar a linguagem de operação e auditoria.

Ela não executa chamadas AWS ou OCI a partir do navegador. O container **Worker AWS/OCI real** executa discovery, S3 Batch Operations, polling e cópia; por segurança começa instalado, porém ocioso. A operação externa só começa após configurar a AWS, preencher o bucket de controle e habilitá-lo explicitamente em **Configurações**. No botão **Discovery**, o operador escolhe entre o discovery remoto normal ou a importação local de um CSV de inventário (inclusive `.csv.gz`), que não chama a AWS. Após a primeira carga, o botão vira **Executar re-discovery**: exige justificativa, preserva evidências de waves existentes, acrescenta objetos novos e registra alterações detectadas. A origem do último inventário fica identificada por uma tag (`API remota`, `S3 Inventory` ou `Arquivo de inventário`). A auditoria profunda é sempre enfileirada manualmente pelo operador após a transferência e exige confirmação reforçada.

## Garantias de desenho

- Uma única VM Linux; PostgreSQL será banco e fila durável.
- Interface web somente em `localhost`, acessível por túnel SSH.
- SSH apenas por chave pública/privada; senha, teclado interativo e login root são desabilitados.
- O Terraform pode criar Vault/Key ou reutilizar recursos existentes; a descoberta de conexões AWS usa apenas Secrets OCI que a VM pode inspecionar e ler.
- Credenciais AWS de bootstrap somente com access key/secret key; a aplicação assume uma role AWS de menor privilégio.
- Restore em lote via S3 Batch Operations; chamadas AWS por objeto são evitadas sempre que houver alternativa bulk.
- Integridade padrão por objeto: a cópia calcula SHA-256 na leitura do S3 e o OCI valida SHA-256 antes de aceitar um `PutObject` pequeno ou cada parte de um multipart. A evidência de aceitação nativa fica persistida sem reler o destino. Para objetos grandes, `upload_id`, tamanho de parte e partes aceitas são checkpoints persistentes: uma interrupção retoma somente as partes ausentes. O tamanho-base multipart é configurável em **Configurações → Configuração operacional** (64 MiB por padrão); a plataforma o ajusta automaticamente quando necessário para não ultrapassar o limite OCI de 10.000 partes. ETag S3 não é tratado como MD5 universal.
- Reconciliação final sob demanda: lista o destino OCI por chave e tamanho e usa `HeadObject` apenas nos candidatos para conferir a proveniência da origem preservada nos metadados; não relê payload nem chama AWS.
- Observabilidade local: Status mostra falhas/retries, checkpoints multipart, transferências estagnadas e espaço do volume; `/metrics` oferece a mesma base no formato Prometheus somente em localhost.
- **Auditoria profunda SHA-256**: operação excepcional que relê todo o objeto OCI e compara o SHA-256 linear. A tela informa volume, tempo mínimo teórico, exige rolar o aviso e marcar confirmação antes de habilitar o início; o Status acompanha o progresso.
- Metadados S3 são copiados para metadados OCI quando compatíveis; tags são preservadas integralmente no PostgreSQL e em uma representação JSON limitada nos metadados OCI.
- Ondas de no máximo 10 TB; referência inicial de VM: 8 OCPUs, 32 GB RAM e 500 GB de boot volume.

## Estrutura

- `terraform/orm`: Stack Terraform para OCI Resource Manager e seu formulário.
- `docs`: arquitetura, deployment, IAM AWS/OCI e plano de validação.

Consulte [arquitetura](docs/architecture.md), [deploy](docs/deployment.md), [AWS Setup](docs/aws-setup.md), [OCI Setup manual](docs/oci-setup.md), [IAM OCI](docs/oci-iam.md), [conexões AWS](docs/aws-connections.md), [estimativas de custo](docs/cost-estimates.md), [plano de validação](docs/validation-test-plan.md), [teste de objetos grandes](docs/large-object-test-runbook.md), [recuperação](docs/recovery-runbook.md) e [cleanup controlado de testes](docs/test-cleanup.md).

Para uma visão consolidada e mantida das capacidades e do modo de operação da
plataforma, consulte [Capacidades e operação do RAIJIN](docs/capabilities.md).

## Uso da console web

Crie um túnel SSH até a VM e mantenha o terminal aberto:

```bash
ssh -N -L 8080:127.0.0.1:8080 -i ~/.ssh/id_rsa opc@<ip-da-vm>
```

Abra `http://127.0.0.1:8080`. A porta 8080 permanece vinculada apenas ao localhost da VM.
