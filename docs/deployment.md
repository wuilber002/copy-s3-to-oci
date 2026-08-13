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
5. Aplique o stack.
6. No Console OCI, crie uma nova versão para cada Secret, substituindo o placeholder pelo valor real. Não altere o placeholder via Terraform.
7. Confirme que a policy automática de backup está associada ao boot volume.

## Acesso local à interface

Na estação administrativa, crie um túnel SSH:

```bash
ssh -N -L 8080:127.0.0.1:8080 <usuario>@<ip-ou-hostname-da-vm>
```

Depois, acesse `http://127.0.0.1:8080`. A porta da aplicação não deve ser liberada no NSG/security list.

## Instalação de release

O procedimento final baixa uma release versionada do GitHub e verifica o checksum antes da instalação. A release inclui imagens Docker e dependências; a VM não depende de Docker Hub, PyPI ou `apt` durante a instalação ou execução.
