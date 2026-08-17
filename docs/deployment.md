# Provisionamento e bootstrap

## Pré-requisitos do cliente

- Subnet existente com saída HTTPS para AWS S3/STS, OCI Vault/Object Storage e GitHub Releases.
- Acesso SSH à VM pela rede corporativa.
- Permissões para executar o stack no OCI Resource Manager e criar os recursos selecionados.
- Policy automática de backup do boot volume existente, ou autorização para criá-la/associá-la.

## OCI Resource Manager

1. Crie um Stack a partir de `terraform/orm` neste repositório.
2. Preencha o formulário. Use 8 OCPUs, 32 GB e boot volume de 500 GB como ponto de partida.
   A VM aceita administração SSH exclusivamente por chave pública/privada: senha, teclado interativo e login direto de root são desabilitados pelo cloud-init e reforçados pelo bootstrap. A porta 8080 nunca deve ser liberada; ela é publicada apenas em `127.0.0.1` na VM. O cliente pode manter a regra de SSH sem restrição de CIDR conforme sua política, desde que preserve a proteção da chave privada.
3. Mantenha a criação de Vault, Key e Secrets: esses recursos são obrigatórios e sempre criados.
4. Se criar policy, informe os buckets OCI de destino em `destination_buckets_json`. Agrupe buckets no mesmo compartment sempre que possível.
5. Aplique o stack. Por padrão, ele cria e associa uma policy de backup incremental diário do boot volume, com retenção de 14 dias. É possível informar uma policy existente em vez disso.
6. No Console OCI, crie uma nova versão para cada Secret, substituindo o placeholder pelo valor real. Não altere o placeholder via Terraform.
7. Confirme que a policy automática de backup está associada ao boot volume.

Depois do deploy, abra **Configurações → Inventário de buckets OCI** e use **Atualizar buckets OCI**. A consulta ocorre somente sob demanda via OCI Resource Search no tenancy e o resultado é persistido no PostgreSQL. O cadastro de origem aceita apenas um bucket presente nesse cache; a policy da Dynamic Group continua sendo a autorização efetiva para escrita.

Uma origem com apenas cadastro, discovery, inventário ou ondas ainda não executadas pode ser excluída definitivamente, removendo também esses dados de preview. Depois que um worker assumir qualquer onda, a interface disponibiliza somente **Arquivar**: ela pausa ondas não concluídas, remove a origem da lista diária e mantém todo o histórico para auditoria.

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
- criação manual de uma onda, criação automática por tamanho e criação automática restrita a um prefixo S3, todas de no máximo 10 TB, com tier e duração de restore;
- prévia local antes da criação automática, com objetos, bytes, estimativa de ondas e alerta para objetos acima do tamanho alvo;
- download do manifesto CSV da onda, já com as chaves codificadas para S3 Batch Operations;
- relatório de objetos, bytes, estados, tarefas, tentativas e erros por onda;
- pausa, retomada, reprocessamento e exclusão controlada de ondas ainda não executadas; e
- saúde do banco/fila e espaço livre do volume, além de recuperação de tarefas cujo lease expirou após interrupção; e
- histórico operacional persistente para ações administrativas e de fila.

Os formulários e indicadores principais possuem ícones `i` de ajuda contextual. Eles descrevem limites, impacto operacional e, quando aplicável, impactos de custo e prazo de restore. A ajuda está disponível por mouse e teclado.

Os parâmetros operacionais são persistidos no PostgreSQL. O **worker AWS/OCI real** é um container separado da API e fica inerte por padrão; somente depois da habilitação explícita ele assume discovery e tarefas duráveis de restore, polling e transferência. O worker de simulação é uma ferramenta de teste e nunca chama AWS ou OCI.

O painel de saúde também mostra o estado do serviço systemd da plataforma, dos containers PostgreSQL e aplicação, do timer de backup lógico e do timer que atualiza esse estado. O host gera um pequeno JSON em `/run/s3-oci-migration` a cada minuto; o container web apenas o lê, sem acesso ao socket Podman, systemd ou privilégios de host.

O bloco **Observabilidade operacional** acompanha, sem chamar AWS ou OCI: tarefas falhas e em retry, leases vencidos, checkpoints multipart pendentes de retomada, transferências sem progresso há mais de dez minutos, falhas persistidas nas últimas 24 horas e espaço livre do volume persistente. Para um coletor local compatível, `http://127.0.0.1:8080/metrics` expõe as métricas essenciais no formato Prometheus; mantenha-o atrás do mesmo túnel SSH ou de um agente local, nunca em uma porta pública.

O cartão **Credenciais e integrações** no Status e o botão **Executar pré-check OCI** em Configurações usam a identidade dinâmica da VM para ler a versão atual de cada Secret e listar no máximo um objeto em cada bucket OCI já cadastrado. Quando todas as credenciais AWS estão preenchidas, o pré-check também executa `GetCallerIdentity` e `AssumeRole` na role de migração; ele não lista, restaura, baixa nem cobra operações de S3. Valores de Secret nunca entram na resposta, nos logs ou na tela.

- **Vermelho** (`PLACEHOLDER`): o valor ainda é o texto instrutivo criado pelo Terraform, ou há uma configuração ausente.
- **Amarelo** (`CONFIGURED`): o valor foi preenchido, porém não existe uma validação segura sem executar uma operação real. A role de Batch Operations só será validada ao criar o primeiro job Batch.
- **Verde** (`VALIDATED` ou `READY`): credencial/integracão testada com sucesso. As duas Secrets da credencial AWS e o ARN da role de migração ficam verdes após STS e `AssumeRole`; namespace e bucket OCI ficam verdes após a leitura autorizada.

A configuração de runtime com os OCIDs é criada pelo cloud-init e montada somente-leitura no container.

### Secret `postgres_password`

O Terraform gera uma senha aleatória de 48 caracteres para o usuário local `migration`, persiste-a no OCI Vault e a VM a lê com sua identidade dinâmica antes de inicializar o PostgreSQL. Não é uma credencial AWS, não é exibida na interface e não pode ser reutilizada. O arquivo local materializado tem permissão `0600` e apenas permite que a VM volte a iniciar sem depender de uma consulta ao Vault a cada reboot. Não altere a versão no Vault diretamente: a rotação deve atualizar coordenadamente o Vault, o PostgreSQL e o arquivo local com `scripts/sync-postgres-password-from-vault.sh`.

Como o valor gerado pelo provider `random` fica no state do Terraform, o state do OCI Resource Manager deve permanecer restrito aos operadores autorizados do stack.

O discovery produtivo usa somente `ListObjectsV2` paginado: registra chave, tamanho, ETag, classe de armazenamento e última modificação sem restaurar, baixar, fazer `HeadObject` ou listar tags. Metadados e tags são lidos somente no momento da cópia de cada objeto já restaurado. As ações da console apenas registram ou enfileiram trabalho durável; não causam chamadas AWS pelo navegador.

Em caso de desligamento, reinicie `s3-oci-migration.service`. PostgreSQL mantém inventário, fila, leases e evidências; tarefas com lease expirado são reassumidas pelo worker. Para uploads grandes, o `upload_id` multipart OCI e as partes já aceitas ficam no PostgreSQL: após reinício, o worker consulta essas partes e envia somente as faltantes. O backup lógico diário é complementar ao backup de volume OCI e pode ser restaurado conforme o procedimento de recuperação desta documentação.

## Instalação de release

O procedimento final baixa uma release versionada do GitHub e verifica o checksum antes da instalação. A release inclui imagens Docker e dependências; a VM não depende de Docker Hub, PyPI ou `apt` durante a instalação ou execução.

Na Oracle Linux, o bootstrap utiliza Podman nativo e registra `s3-oci-migration.service` no systemd. Os containers `s3-oci-postgres` e `s3-oci-app` usam volumes persistentes no boot volume. A API é publicada apenas em `127.0.0.1:8080`.

O timer `s3-oci-backup-postgres.timer` executa diariamente às 02:15 UTC um `pg_dump` local e conserva 14 backups. Isso complementa, mas não substitui, a policy automática de backup do boot volume.
