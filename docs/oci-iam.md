# IAM OCI

O stack pode criar a Dynamic Group e a Policy. As duas opções existem para ambientes cujo time de IAM exige criar esses recursos manualmente.

## Dynamic Group

A Dynamic Group deve conter exclusivamente a VM de migração:

```text
ALL {instance.id = '<VM_OCID>'}
```

## Policy mínima

Para `N` compartments distintos que guardam buckets de destino, a policy possui `N + 2` statements:

1. um para ler os Secret Bundles;
2. um para consultar o namespace do Object Storage;
3. um por compartment distinto de bucket, consolidando todos os buckets daquele compartment.

Exemplo com três buckets em dois compartments:

```text
Allow dynamic-group s3-oci-migration-vm to read secret-bundles in compartment id <SECRETS_COMPARTMENT_OCID>
Allow dynamic-group s3-oci-migration-vm to read objectstorage-namespaces in tenancy

Allow dynamic-group s3-oci-migration-vm to manage objects in compartment id <COMPARTMENT_A_OCID> where any {target.bucket.name='destination-finance', target.bucket.name='destination-legal'}
Allow dynamic-group s3-oci-migration-vm to manage objects in compartment id <COMPARTMENT_B_OCID> where target.bucket.name='destination-technology'
```

## Aviso sobre limites

OCI impõe limites para statements de policy por ramificação de compartments. Escolha o compartment de criação da policy de modo que ele governe todos os compartments informados e agrupe destinos no menor número possível de compartments. Cada novo bucket no mesmo compartment não aumenta a quantidade de statements; cada novo compartment aumenta um.

O stack mostra `policy_statement_count` como saída após o apply.

## Permissões para Resource Manager

O operador do stack precisa de permissões para criar Compute, Vault, Keys, Secrets e, quando as flags estiverem ativas, Dynamic Group, Policy e associação da policy de backup do boot volume. O time IAM do cliente deve conceder essas permissões somente nos compartments selecionados.
