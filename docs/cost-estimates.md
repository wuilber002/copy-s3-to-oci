# Estimativas de custo por wave

RAIJIN estima unidades de consumo conhecidas da plataforma para cada wave. A
estimativa é uma ferramenta de planejamento e aprovação operacional; não é
uma previsão de fatura nem substitui a calculadora, contrato ou relatório de
cobrança AWS/OCI do cliente.

## Configuração das tarifas

Em **Configurações → Conexões AWS → Tarifas**, cadastre valores aplicáveis à
conta, região e contrato daquela conexão. A tabela é por conexão porque contas
podem estar em regiões diferentes ou ter preços negociados distintos.

Preencha os preços sem impostos, na mesma moeda. Por padrão a moeda é `USD`.
O campo **Referência** deve registrar a origem e a data, por exemplo:

```text
AWS S3 Pricing, sa-east-1, contrato cliente, revisado em 2026-08-18
```

Uma tarifa deixada em branco não é assumida como zero: o componente aparece
como **não estimado** e o custo único total da wave também fica indisponível se
aquele componente for necessário. O valor `0` é aceito somente quando o
operador confirmou que aquela operação é gratuita no contrato/tier escolhido.

## Componentes calculados

O botão **Custo estimado** da wave mostra quantidade, unidade, tarifa e valor
de cada componente:

- Job e tarefas de S3 Batch Operations para objetos arquivados;
- escrita do manifesto e páginas `ListObjectsV2` do discovery;
- polling de restore. O cálculo usa o número de páginas da origem multiplicado
  pelos ciclos de polling configurados na tabela de tarifas;
- retrieval de `GLACIER` e `DEEP_ARCHIVE`, separado por tier `BULK` ou
  `STANDARD`;
- cópia temporária restaurada em S3 Standard durante os dias de retenção;
- leituras de objetos e, quando habilitado, leituras de tags no S3;
- bytes de saída AWS para OCI;
- operações de escrita OCI. Objetos pequenos contam um `PutObject`; multipart
  conta criação, partes estimadas e commit usando o tamanho de parte atual;
- armazenamento mensal no bucket OCI de destino, mostrado separadamente como
  custo recorrente;
- auditoria profunda SHA-256 como cenário opcional separado. Ela não entra no
  total de migração, pois só deve ser iniciada sob demanda.

## Limites e interpretação

RAIJIN conhece os objetos, tamanhos, classes e retenção de cada wave, portanto
as quantidades principais são determinísticas. Ainda assim, os valores podem
variar por fatores externos:

- descontos, créditos, impostos e faixas de uso agregadas da conta AWS;
- duração real do restore, que altera a quantidade de polls;
- alterações de tabela pública de preços;
- classes de archive não representadas por uma tarifa específica. Essas classes
  são destacadas e impedem que a estimativa seja apresentada como completa;
- operações que o cliente executa fora do Raijin.

Conserve a referência da tarifa usada antes de colocar uma wave grande na fila.
O relatório JSON da wave pode ser baixado junto das evidências operacionais;
a estimativa pode ser consultada novamente a qualquer momento com a tabela de
tarifas atual.
