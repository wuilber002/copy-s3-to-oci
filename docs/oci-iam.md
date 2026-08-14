# IAM OCI

O stack pode criar a Dynamic Group e a Policy. As duas opções existem para ambientes cujo time de IAM exige criar esses recursos manualmente.

## Dynamic Group

A Dynamic Group deve conter exclusivamente a VM de migração:

```text
ALL {instance.id = '<VM_OCID>'}
```

## Policy mínima

Para `N` compartments distintos que guardam buckets de destino, a policy possui `1 + 2N` statements:

1. um para ler os Secret Bundles;
2. um `inspect buckets` por compartment de destino, para que o OCI Resource Search retorne apenas metadados desses compartments;
3. um `manage objects` por compartment distinto de bucket, consolidando todos os buckets autorizados naquele compartment.

Exemplo com três buckets em dois compartments:

```text
Allow dynamic-group s3-oci-migration-vm to read secret-bundles in compartment id <SECRETS_COMPARTMENT_OCID>
Allow dynamic-group s3-oci-migration-vm to inspect buckets in compartment id <COMPARTMENT_A_OCID>
Allow dynamic-group s3-oci-migration-vm to manage objects in compartment id <COMPARTMENT_A_OCID> where any {target.bucket.name='destination-finance', target.bucket.name='destination-legal'}
Allow dynamic-group s3-oci-migration-vm to inspect buckets in compartment id <COMPARTMENT_B_OCID>
Allow dynamic-group s3-oci-migration-vm to manage objects in compartment id <COMPARTMENT_B_OCID> where target.bucket.name='destination-technology'
```

## Aviso sobre limites

OCI impõe limites para statements de policy por ramificação de compartments. Escolha o compartment de criação da policy de modo que ele governe todos os compartments informados e agrupe destinos no menor número possível de compartments. Cada novo bucket no mesmo compartment não aumenta a quantidade de statements; cada novo compartment aumenta um.

O OCI Resource Search é executado sobre o tenancy, mas retorna somente metadados de buckets nos compartments onde a identidade dinâmica recebeu `inspect buckets`. Não há statement `in tenancy`: o namespace e o mapeamento de OCID para nome dos compartments de destino são fornecidos pelo Terraform no arquivo local de runtime da VM. O acesso efetivo continua limitado às regras `manage objects` dos buckets explicitamente aprovados. O stack mostra `policy_statement_count` como saída após o apply.

## Permissões para Resource Manager

O operador do stack precisa de permissões para criar Compute, Vault, Keys, Secrets e, quando as flags estiverem ativas, Dynamic Group, Policy e associação da policy de backup do boot volume. O time IAM do cliente deve conceder essas permissões somente nos compartments selecionados.
