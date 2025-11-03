# Exercício Final - Pipeline de Dados de Produtos

## 🎯 Objetivo
Criar um pipeline ETL completo para processar dados de produtos e vendas, aplicando os conceitos aprendidos na aula.

## 📋 Cenário
Você trabalha como engenheiro de dados em uma empresa de e-commerce. Precisa criar um pipeline que processe dados de produtos vindos de diferentes fontes e gere relatórios consolidados.

## 📊 Dados Fornecidos

### Arquivo: `produtos_loja.csv`
```csv
ID_Produto,Nome_Produto,Categoria,Preco_Custo,Fornecedor,Status
P001,Notebook Dell,Eletrônicos,2500.00,Dell Inc,Ativo
P002,Mouse Logitech,Acessórios,45.50,Logitech,Ativo
P003,Teclado Mecânico,Acessórios,,Razer,Inativo
P004,Monitor 24",Eletrônicos,800.00,Samsung,Ativo
P005,Webcam HD,Acessórios,120.00,,Ativo
```

### Arquivo: `vendas_produtos.csv`
```csv
ID_Venda,ID_Produto,Quantidade_Vendida,Preco_Venda,Data_Venda,Canal_Venda
V001,P001,2,3200.00,2024-01-15,Online
V002,P002,5,55.00,2024-01-15,Loja Física
V003,P001,1,3200.00,2024-01-16,Online
V004,P004,3,950.00,2024-01-16,Online
V005,P002,10,,2024-01-17,Loja Física
```

## 🚀 Tarefas do Exercício

### Parte 1: Análise e Planejamento
1. **Identifique os problemas nos dados:**
   - Quais campos têm valores nulos?
   - Que transformações são necessárias?

2. **Defina a estratégia ETL:**
   - Qual abordagem usar: ETL ou ELT? Justifique.
   - Que transformações aplicar?

### Parte 2: Implementação da DAG
Crie uma DAG chamada `pipeline_produtos_vendas` com as seguintes tarefas:

#### Task 1: `extract_produtos`
- Ler arquivo `produtos_loja.csv`
- Validar se o arquivo existe
- Log do número de registros extraídos

#### Task 2: `extract_vendas`
- Ler arquivo `vendas_produtos.csv`
- Validar se o arquivo existe
- Log do número de registros extraídos

#### Task 3: `transform_data`
- **Limpeza de dados:**
  - Preencher `Preco_Custo` nulo com média da categoria
  - Preencher `Fornecedor` nulo com "Não Informado"
  - Preencher `Preco_Venda` nulo com `Preco_Custo * 1.3`

- **Transformações:**
  - Calcular `Receita_Total` = `Quantidade_Vendida * Preco_Venda`
  - Calcular `Margem_Lucro` = `Preco_Venda - Preco_Custo`
  - Criar campo `Mes_Venda` extraído de `Data_Venda`

#### Task 4: `create_tables`
- Criar tabela `produtos_processados`
- Criar tabela `vendas_processadas`
- Criar tabela `relatorio_vendas` (join dos dados)

#### Task 5: `load_data`
- Carregar dados transformados nas tabelas PostgreSQL
- Validar se os dados foram inseridos corretamente

#### Task 6: `generate_report`
- Gerar relatório com:
  - Total de vendas por categoria
  - Produto mais vendido
  - Canal de venda com maior receita
  - Margem de lucro média por categoria

### Parte 3: Configuração e Execução
1. **Configurar a DAG:**
   - Schedule: diário às 6h da manhã
   - Retry: 2 tentativas
   - Email on failure: False
   - Tags: ['produtos', 'vendas', 'exercicio']

2. **Executar e validar:**
   - Executar a DAG manualmente
   - Verificar logs de cada tarefa
   - Validar dados no PostgreSQL

## 📝 Estrutura das Tabelas

### `produtos_processados`
```sql
CREATE TABLE produtos_processados (
    ID_Produto VARCHAR(10),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Preco_Custo DECIMAL(10,2),
    Fornecedor VARCHAR(100),
    Status VARCHAR(20),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `vendas_processadas`
```sql
CREATE TABLE vendas_processadas (
    ID_Venda VARCHAR(10),
    ID_Produto VARCHAR(10),
    Quantidade_Vendida INTEGER,
    Preco_Venda DECIMAL(10,2),
    Data_Venda DATE,
    Canal_Venda VARCHAR(20),
    Receita_Total DECIMAL(10,2),
    Mes_Venda VARCHAR(7),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `relatorio_vendas`
```sql
CREATE TABLE relatorio_vendas (
    ID_Venda VARCHAR(10),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Quantidade_Vendida INTEGER,
    Receita_Total DECIMAL(10,2),
    Margem_Lucro DECIMAL(10,2),
    Canal_Venda VARCHAR(20),
    Mes_Venda VARCHAR(7),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## ✅ Critérios de Avaliação

### Conceitos (30 pontos)
- [ ] Justificativa correta da escolha ETL vs ELT
- [ ] Identificação adequada dos problemas nos dados
- [ ] Estratégia de transformação bem definida

### Implementação (50 pontos)
- [ ] DAG estruturada corretamente
- [ ] Tarefas implementadas conforme especificação
- [ ] Tratamento adequado de dados nulos
- [ ] Cálculos corretos (receita, margem, etc.)
- [ ] Dependências entre tarefas bem definidas

### Execução (20 pontos)
- [ ] DAG executa sem erros
- [ ] Dados carregados corretamente no PostgreSQL
- [ ] Logs informativos em cada etapa
- [ ] Validações implementadas

## 🎁 Desafio Bônus (+10 pontos)
Implemente uma tarefa adicional que:
- Detecte produtos com baixa performance (menos de 2 vendas)
- Envie alerta por log
- Crie tabela `produtos_baixa_performance`

## 📚 Recursos de Apoio
- Documentação do Pandas para transformações
- Airflow TaskFlow API para dependências
- PostgreSQL Hooks para conexão com banco
- Logs do Airflow para debugging

## 📤 Forma de Entrega

### Instruções para Entrega
1. **Fork do Repositório:**
   - Faça um fork deste repositório para sua conta GitHub

2. **Estrutura da Solução:**
   - Crie uma pasta com seu **RA** na raiz do projeto
   - Exemplo: se seu RA é `123456`, crie a pasta `123456/`
   - Coloque todos os arquivos da sua solução dentro desta pasta

3. **Arquivos Obrigatórios:**
   - `pipeline_produtos_vendas.py` (sua DAG)
   - `README.md` (documentação da sua solução)
   - Arquivos CSV de dados (se modificados)

4. **Pull Request:**
   - Título: `[RA] - [SEU_NOME_COMPLETO] - Exercício Final`
   - Exemplo: `[123456] - João Silva Santos - Exercício Final`
   - Descrição: breve resumo da sua implementação

### Estrutura Esperada
```
data-pipeline-workshop-airflow3/
├── [SEU_RA]/
│   ├── pipeline_produtos_vendas.py
│   ├── README.md
│   └── dados/ (se necessário)
└── ...
```

---

*Lembre-se: O objetivo é aplicar os conceitos de pipeline de dados de forma prática. Foque na qualidade da implementação e na clareza do código.*