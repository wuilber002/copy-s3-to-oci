# S3 Glacier to OCI Object Storage Migration

Plataforma de migração controlada de objetos AWS S3, incluindo Glacier Deep Archive, para OCI Object Storage. Ela opera em uma única VM OCI e mantém inventário, ondas, estado de restore, transferências e evidências de integridade em PostgreSQL.

## Estado atual

O repositório contém a infraestrutura para OCI Resource Manager e uma console web local para operar o plano de migração. A console permite cadastrar fontes, consultar inventário paginado, criar uma onda manual, gerar ondas automaticamente por tamanho ou por prefixo S3, baixar manifestos CSV para S3 Batch Operations, exportar relatórios, pausar/retomar/reprocessar/excluir ondas não executadas e acompanhar a fila durável, a saúde local, o estado dos serviços da VM, a prontidão OCI e o histórico operacional.

O procedimento de recursos AWS por AWS CloudShell/CLI está em [docs/aws-cli-setup.md](docs/aws-cli-setup.md). Ele é documentação; não há scripts de provisionamento AWS publicados no repositório.

A console separa o trabalho em **Status** (alertas, fila, transferências e auditorias em andamento), **Migrações** (fontes, inventário e ondas) e **Configurações** (ícone de engrenagem; parâmetros globais e pré-check OCI).

Ela não executa chamadas AWS ou OCI a partir do navegador. O container **Worker AWS/OCI real** executa discovery, S3 Batch Operations, polling e cópia; por segurança começa instalado, porém ocioso. A operação externa só começa após configurar a AWS, preencher o bucket de controle e habilitá-lo explicitamente em **Configurações**. A auditoria profunda é sempre enfileirada manualmente pelo operador após a transferência e exige confirmação reforçada.

## Garantias de desenho

- Uma única VM Linux; PostgreSQL será banco e fila durável.
- Interface web somente em `localhost`, acessível por túnel SSH.
- Vault, Key e Secrets OCI sempre criados pelo Terraform, com placeholders instrutivos.
- Credenciais AWS de bootstrap somente com access key/secret key; a aplicação assume uma role AWS de menor privilégio.
- Restore em lote via S3 Batch Operations; chamadas AWS por objeto são evitadas sempre que houver alternativa bulk.
- Integridade padrão por objeto: a cópia calcula SHA-256 na leitura do S3 e o OCI valida SHA-256 antes de aceitar um `PutObject` pequeno ou cada parte de um multipart. A evidência de aceitação nativa fica persistida sem reler o destino. ETag S3 não é tratado como MD5 universal.
- **Auditoria profunda SHA-256**: operação excepcional que relê todo o objeto OCI e compara o SHA-256 linear. A tela informa volume, tempo mínimo teórico, exige rolar o aviso e marcar confirmação antes de habilitar o início; o Status acompanha o progresso.
- Metadados S3 são copiados para metadados OCI quando compatíveis; tags são preservadas integralmente no PostgreSQL e em uma representação JSON limitada nos metadados OCI.
- Ondas de no máximo 10 TB; referência inicial de VM: 8 OCPUs, 32 GB RAM e 500 GB de boot volume.

## Estrutura

- `terraform/orm`: Stack Terraform para OCI Resource Manager e seu formulário.
- `docs`: arquitetura, deployment, IAM AWS/OCI e plano de validação.

Consulte [arquitetura](docs/architecture.md), [deploy](docs/deployment.md), [IAM AWS](docs/aws-setup.md), [IAM OCI](docs/oci-iam.md) e [plano de validação](docs/validation-test-plan.md).

## Uso da console web

Crie um túnel SSH até a VM e mantenha o terminal aberto:

```bash
ssh -N -L 8080:127.0.0.1:8080 -i ~/.ssh/id_rsa opc@<ip-da-vm>
```

Abra `http://127.0.0.1:8080`. A porta 8080 permanece vinculada apenas ao localhost da VM.
