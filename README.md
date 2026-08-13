# S3 Glacier to OCI Object Storage Migration

Plataforma de migração controlada de objetos AWS S3, incluindo Glacier Deep Archive, para OCI Object Storage. Ela opera em uma única VM OCI e mantém inventário, ondas, estado de restore, transferências e evidências de integridade em PostgreSQL.

## Estado atual

O repositório contém a fundação de infraestrutura para OCI Resource Manager e a documentação de pré-requisitos. A aplicação web e os workers de migração serão adicionados nas próximas etapas.

## Garantias de desenho

- Uma única VM Linux; PostgreSQL será banco e fila durável.
- Interface web somente em `localhost`, acessível por túnel SSH.
- Vault, Key e Secrets OCI sempre criados pelo Terraform, com placeholders instrutivos.
- Credenciais AWS de bootstrap somente com access key/secret key; a aplicação assume uma role AWS de menor privilégio.
- Restore em lote via S3 Batch Operations; chamadas AWS por objeto são evitadas sempre que houver alternativa bulk.
- Ondas de no máximo 10 TB; referência inicial de VM: 8 OCPUs, 32 GB RAM e 500 GB de boot volume.

## Estrutura

- `terraform/orm`: Stack Terraform para OCI Resource Manager e seu formulário.
- `docs`: arquitetura, deployment, IAM AWS/OCI e plano da PoC.

Consulte [arquitetura](docs/architecture.md), [deploy](docs/deployment.md), [IAM AWS](docs/aws-setup.md), [IAM OCI](docs/oci-iam.md) e [PoC](docs/poc-test-plan.md).
