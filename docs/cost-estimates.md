# Estimativas de custo por wave

RAIJIN estima unidades de consumo conhecidas da plataforma para cada wave. A
estimativa é uma ferramenta de planejamento e aprovação operacional; não é
uma previsão de fatura nem substitui a calculadora, contrato ou relatório de
cobrança AWS/OCI do cliente.

## Preços públicos, contrato e precedência

Primeiro habilite **Configurações → Configuração operacional → Estimativa de
custo por wave**. A chave é global e começa desligada por segurança. Enquanto
estiver desligada, RAIJIN não executa cálculos nem permite abrir estimativas;
as tabelas de tarifas já cadastradas permanecem guardadas.

Ao habilitar a estimativa, RAIJIN passa a manter uma cópia regional da
**AWS Price List pública do Amazon S3 e AWS Data Transfer**. A coleta é feita
diretamente dos endpoints públicos da AWS, sem credenciais AWS, e ocorre
automaticamente a cada 7 dias por padrão. O intervalo pode ser ajustado entre
1 e 90 dias ou a lista pode ser atualizada manualmente em **Configurações →
Lista pública de preços AWS**. A saída AWS→OCI usa especificamente a tarifa
regional `AWS Outbound` para `External`, evitando confundir MRAP e tráfego
entre regiões com egress para OCI.

A precedência é aplicada por campo, para cada conexão:

1. tarifa personalizada da conexão — use para descontos, contratos, créditos
   ou valores específicos da conta;
2. tarifa pública AWS da região da conexão;
3. não estimado, quando nenhuma das duas fontes contém uma tarifa aplicável.

RAIJIN coleta preços para as regiões das conexões e também para regiões ainda
registradas em sources legadas. Isso preserva estimativas corretas quando uma
origem histórica foi criada antes da região da conexão ser sincronizada.

Em **Configurações → Conexões AWS → Tarifas**, preencha somente os valores que
devem substituir a lista pública. A tabela é por conexão porque contas podem
estar em regiões diferentes ou ter preços negociados distintos. Deixar um
campo em branco não remove a estimativa: ele volta a usar a tabela pública,
quando disponível.

Preencha os preços sem impostos, na mesma moeda. Por padrão a moeda é `USD`.
O campo **Referência** deve registrar a origem e a data, por exemplo:

```text
AWS S3 Pricing, sa-east-1, contrato cliente, revisado em 2026-08-18
```

A tabela pública AWS não cobre preços OCI. Para que o custo fim a fim inclua
operações e armazenamento OCI, preencha esses campos na conexão com a tabela
OCI aplicável ao tenancy. Uma tarifa ausente não é assumida como zero: o
componente aparece como **não estimado**, mas os componentes conhecidos passam
a compor um **subtotal parcial** claramente identificado. O valor `0` é aceito
somente quando o operador confirmou que aquela operação é gratuita no
contrato/tier escolhido.

## Componentes calculados
O símbolo **💲** da wave mostra quantidade, unidade, tarifa, fonte da tarifa
(contrato da conexão ou lista pública AWS) e valor de cada componente. O mesmo
símbolo ao lado do resumo de uma source consolida somente as waves já criadas;
objetos ainda sem wave são destacados e não entram no total:

Em cada conexão, **Incluir saída AWS → OCI** começa habilitado. Quando for
desabilitado, o egress AWS→OCI não é tratado como custo zero: ele é removido
integralmente da estimativa, dos totais e da tabela de componentes. Use essa
opção apenas se esse custo não se aplica ao contrato, for absorvido por outro
serviço ou precisar ser tratado fora do Raijin.

**Incluir custos OCI** também começa habilitado em cada conexão. Ao desativá-lo,
operações de escrita, armazenamento mensal e leituras OCI de auditoria profunda
são removidos integralmente da estimativa, dos totais e da tabela de
componentes; eles não são interpretados como custo zero.

- Job e tarefas de S3 Batch Operations para objetos arquivados;
- escrita do manifesto e páginas `ListObjectsV2` do discovery;
- polling de restore. O cálculo usa o número de objetos arquivados da wave
  multiplicado pelos ciclos de polling configurados na tabela de tarifas; cada
  verificação é um `HeadObject` direcionado ao objeto pendente e usa a tarifa
  S3 GET/other requests;
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
