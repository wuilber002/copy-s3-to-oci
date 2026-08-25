# Recuperação operacional

Este roteiro recupera o plano de controle local do Raijin: inventário, fontes,
ondas, fila, checkpoints multipart, eventos, duração acumulada do discovery e evidências de integridade. Ele
**não** remove nem altera objetos S3 ou OCI.

## Recuperação após reboot ou parada acidental

1. Conecte-se à VM por SSH e confirme que a porta web continua local.
2. Execute `sudo systemctl start s3-oci-migration.service`.
3. Confirme saúde com `curl -fsS http://127.0.0.1:8080/healthz`.
4. Abra a console pelo túnel SSH e revise **Status → Observabilidade** e a
   fila. Tarefas cujo lease venceu voltam a ser elegíveis; uploads multipart
   consultam as partes OCI já aceitas antes de enviar partes faltantes.
5. Para uma source que falhou em discovery, execute novamente **Run discovery**.
   Se existir checkpoint, o worker continua no `ContinuationToken` salvo, sem
   relistar páginas que já foram confirmadas no PostgreSQL.

Não reprocese uma wave de Archive apenas por causa de reboot: primeiro confira
o relatório da wave e a tentativa Batch preservada.

## Restauração de backup lógico PostgreSQL

Use somente depois de confirmar que o volume de boot ou o banco local não pode
ser recuperado normalmente. A restauração devolve o banco ao instante do backup
e, portanto, pode exigir reconciliação OCI e revisão das tarefas posteriores.

1. Liste backups: `sudo ls -lht /var/lib/s3-oci-migration/backups/`.
2. Registre o arquivo escolhido e a razão da restauração no ticket operacional.
3. Execute, com caminho absoluto:

   ```bash
   sudo s3-oci-restore-postgres \
     --backup /var/lib/s3-oci-migration/backups/migration-YYYYMMDDTHHMMSSZ.dump \
     --confirm-restore
   ```

Para recuperar o ambiente simulado, use o arquivo
`migration-simulation-YYYYMMDDTHHMMSSZ.dump` no mesmo comando. O utilitário
identifica o banco de destino pelo nome do arquivo. Os dois bancos são
copiados diariamente e a retenção padrão de 35 dias cobre toda a quarentena
padrão de 30 dias.

O comando exige confirmação explícita, aceita somente arquivos do diretório de
backups, cria um backup adicional do estado atual antes de restaurar e aguarda
`/healthz` voltar a responder. Se a API não retornar saudável, não reenvie
restores AWS: investigue `sudo journalctl -u s3-oci-migration.service -n 200`.

Após a recuperação, execute **Validate OCI destination** nas sources com ondas
concluídas e revise a fila antes de retomar qualquer wave.

## Exercício obrigatório antes de produção

Em ambiente de teste, execute um `pg_dump` manual, faça uma alteração inofensiva
na console, restaure o dump e confirme que o inventário, uma wave e o histórico
voltaram ao estado esperado. Registre data, operador, arquivo e resultado.
