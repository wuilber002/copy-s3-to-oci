# Provisionamento e bootstrap

## Pré-requisitos do cliente

- Subnet existente com saída HTTPS para AWS S3/STS, OCI Vault/Object Storage e GitHub Releases.
- Acesso SSH à VM pela rede corporativa.
- Permissões para executar o stack no OCI Resource Manager e criar os recursos selecionados.
- Policy automática de backup do boot volume existente, ou autorização para criá-la/associá-la.

## OCI Resource Manager

1. Crie um Stack a partir de `terraform/orm` neste repositório.
2. Preencha o formulário. Use 8 OCPUs, 32 GB e boot volume de 500 GB como ponto de partida.
   Para uma PoC em subnet pública, habilite `Assign public IP to VM` somente se a security list/NSG restringir a porta 22 à rede administrativa. Em subnet privada, mantenha desligado e use o bastion/VPN do cliente.
3. Mantenha a criação de Vault, Key e Secrets: esses recursos são obrigatórios e sempre criados.
4. Se criar policy, informe os buckets OCI de destino em `destination_buckets_json`. Agrupe buckets no mesmo compartment sempre que possível.
5. Aplique o stack. Por padrão, ele cria e associa uma policy de backup incremental diário do boot volume, com retenção de 14 dias. É possível informar uma policy existente em vez disso.
6. No Console OCI, crie uma nova versão para cada Secret, substituindo o placeholder pelo valor real. Não altere o placeholder via Terraform.
7. Confirme que a policy automática de backup está associada ao boot volume.

## Acesso local à interface

Na estação administrativa, crie um túnel SSH:

```bash
ssh -N -L 8080:127.0.0.1:8080 <usuario>@<ip-ou-hostname-da-vm>
```

Depois, acesse `http://127.0.0.1:8080`. A porta da aplicação não deve ser liberada no NSG/security list.

### Operação da console

A console é o plano de controle local e persiste suas operações no PostgreSQL da VM. Ela oferece:

- cadastro e seleção de uma origem S3 e de seu bucket OCI de destino;
- totais e amostra paginada do inventário que foi descoberto;
- criação de ondas de no máximo 10 TB, com tier e duração de restore;
- download do manifesto CSV da onda, já com as chaves codificadas para S3 Batch Operations;
- relatório de objetos, bytes, estados, tarefas, tentativas e erros por onda;
- pausa, retomada e reprocessamento controlado de ondas; e
- saúde do banco/fila e espaço livre do volume, além de recuperação de tarefas cujo lease expirou após interrupção; e
- histórico operacional persistente para ações administrativas e de fila.

Os formulários e indicadores principais possuem ícones `i` de ajuda contextual. Eles descrevem limites, impacto operacional e, quando aplicável, impactos de custo e prazo de restore. A ajuda está disponível por mouse e teclado.

Os parâmetros operacionais são persistidos no PostgreSQL. O worker simulado é uma ferramenta de PoC: fica instalado, mas inerte por padrão, e só processa tarefas quando a opção explícita for habilitada pela console. Ele nunca chama AWS ou OCI; apenas percorre os estados fictícios de restore, polling e transferência para validar leases, retomada e auditoria.

O painel de saúde também mostra o estado do serviço systemd da plataforma, dos containers PostgreSQL e aplicação, do timer de backup lógico e do timer que atualiza esse estado. O host gera um pequeno JSON em `/run/s3-oci-migration` a cada minuto; o container web apenas o lê, sem acesso ao socket Podman, systemd ou privilégios de host.

O cartão **Credenciais e integrações** no Painel e o botão **Executar pré-check OCI** em Configurações usam a identidade dinâmica da VM para ler a versão atual de cada Secret e listar no máximo um objeto em cada bucket OCI já cadastrado. Quando todas as credenciais AWS estão preenchidas, o pré-check também executa `GetCallerIdentity` e `AssumeRole` na role de migração; ele não lista, restaura, baixa nem cobra operações de S3. Valores de Secret nunca entram na resposta, nos logs ou na tela.

- **Vermelho** (`PLACEHOLDER`): o valor ainda é o texto instrutivo criado pelo Terraform, ou há uma configuração ausente.
- **Amarelo** (`CONFIGURED`): o valor foi preenchido, porém não existe uma validação segura sem executar uma operação real. A role de Batch Operations só será validada ao criar o primeiro job Batch.
- **Verde** (`VALIDATED` ou `READY`): credencial/integracão testada com sucesso. As duas Secrets da credencial AWS e o ARN da role de migração ficam verdes após STS e `AssumeRole`; namespace e bucket OCI ficam verdes após a leitura autorizada.

A configuração de runtime com os OCIDs é criada pelo cloud-init e montada somente-leitura no container.

Neste estágio, discovery, restore, cópia e verificação ainda dependem do worker AWS/OCI, que será habilitado após a configuração dos Secrets e do ambiente AWS. O discovery produtivo será feito por listagens S3 paginadas e não haverá cadastro manual de objetos pela interface. As ações da console apenas registram ou enfileiram trabalho durável; não causam chamadas AWS pelo navegador.

## Instalação de release

O procedimento final baixa uma release versionada do GitHub e verifica o checksum antes da instalação. A release inclui imagens Docker e dependências; a VM não depende de Docker Hub, PyPI ou `apt` durante a instalação ou execução.

Na Oracle Linux, o bootstrap utiliza Podman nativo e registra `s3-oci-migration.service` no systemd. Os containers `s3-oci-postgres` e `s3-oci-app` usam volumes persistentes no boot volume. A API é publicada apenas em `127.0.0.1:8080`.

O timer `s3-oci-backup-postgres.timer` executa diariamente às 02:15 UTC um `pg_dump` local e conserva 14 backups. Isso complementa, mas não substitui, a policy automática de backup do boot volume.
