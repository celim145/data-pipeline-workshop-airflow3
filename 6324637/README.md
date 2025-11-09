# pipeline_produtos_vendas

Esta pasta contém a solução do Exercício Final (exemplo RA = `123456`).

Arquivos:

- `pipeline_produtos_vendas.py`: DAG Airflow implementando as tasks de extração, transformação, criação de tabelas, carregamento e geração de relatório. Também inclui a tarefa bônus para detectar produtos de baixa performance.

Principais suposições
- Os arquivos CSV originais estão em `data/produtos_loja.csv` e `data/vendas_produtos.csv`.
- Existe uma connection no Airflow chamada `postgres_default` apontando para o banco PostgreSQL de destino.

Como usar

1. Copie a pasta `123456/` para seu DAGs folder do Airflow ou deixe-a no repositório (o arquivo `dags/pipeline_produtos_vendas.py` já foi criado também).
2. Configure a connection `postgres_default` em Admin -> Connections do Airflow (tipo Postgres).
   - Preencha host, schema (database), login, password e port.
3. Reinicie o scheduler/webserver se necessário. A DAG `pipeline_produtos_vendas` aparecerá com agendamento diário às 06:00.
4. Execute manualmente (Trigger DAG) para testar.

Validações implementadas
- Cada etapa registra logs do número de linhas processadas.
- Após o carregamento, o DAG executa consultas de validação (SELECT COUNT(*)) para as tabelas: `produtos_processados`, `vendas_processadas`, `relatorio_vendas`, `produtos_baixa_performance`.

Notas técnicas e limitações
- A função de escrita em Postgres tenta usar COPY via cursor.copy_expert (rápido). Se houver erro, o código faz fallback para `insert_rows`.
- Os dados trafegam entre tasks via XCom em formato JSON (pequenas amostras). Em pipelines maiores, prefira armazenamento intermediário (S3 / GCS / banco).
- Caso queira alterar o `conn_id`, edite a constante `DEFAULT_CONN_ID` no topo do arquivo `pipeline_produtos_vendas.py`.

Teste local rápido (sem Airflow)
- Você pode testar as transformações localmente importando as funções de transformação: abrir um Python e executar um pequeno script que injeta os CSVs e chama `transform_data` (ou adaptar o código do DAG para um script de teste). O DAG foi escrito para rodar no Airflow.


