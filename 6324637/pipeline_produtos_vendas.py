from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook


# Assumptions:
# - CSV files are located in the repository `data/` folder.
# - A Postgres connection with conn_id 'postgres_default' exists in Airflow.

DEFAULT_CONN_ID = "postgres_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _find_data_path(filename: str) -> str:
    # try relative to this file, then repo root cwd, then data/
    here = os.path.dirname(__file__)
    candidates = [
        os.path.join(here, "..", "data", filename),
        os.path.join(here, "..", "..", "data", filename),
        os.path.join(os.getcwd(), "data", filename),
        os.path.join("/opt/airflow", "data", filename),
        filename,
    ]
    for p in candidates:
        p = os.path.abspath(p)
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"File {filename} not found in known locations: {candidates}")


def _df_to_postgres_copy(df: pd.DataFrame, table_name: str, pg_hook: PostgresHook):
    # Use COPY ... FROM STDIN WITH CSV for fast bulk insert
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # Ensure columns order
    cols = list(df.columns)
    sio = StringIO()
    df.to_csv(sio, index=False, header=False)
    sio.seek(0)

    copy_sql = f"COPY {table_name} ({', '.join(cols)}) FROM STDIN WITH CSV"
    cur.copy_expert(sql=copy_sql, file=sio)
    conn.commit()


@dag(
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=["produtos", "vendas", "exercicio"],
)
def pipeline_produtos_vendas():
    """DAG: pipeline_produtos_vendas

    Steps:
    - extract_produtos
    - extract_vendas
    - transform_data
    - create_tables
    - load_data
    - generate_report
    - bonus: produtos_baixa_performance
    """

    @task()
    def extract_produtos() -> str:
        path = _find_data_path("produtos_loja.csv")
        logging.info(f"Reading produtos file from: {path}")
        df = pd.read_csv(path)
        logging.info(f"Extracted produtos rows={len(df)}")
        return df.to_json(orient="records", date_format="iso")

    @task()
    def extract_vendas() -> str:
        path = _find_data_path("vendas_produtos.csv")
        logging.info(f"Reading vendas file from: {path}")
        df = pd.read_csv(path)
        logging.info(f"Extracted vendas rows={len(df)}")
        return df.to_json(orient="records", date_format="iso")

    @task()
    def transform_data(produtos_json: str, vendas_json: str) -> dict:
        produtos = pd.read_json(produtos_json, convert_dates=False)
        vendas = pd.read_json(vendas_json, convert_dates=False)

        # Normalize numeric columns
        produtos["Preco_Custo"] = pd.to_numeric(produtos["Preco_Custo"], errors="coerce")
        vendas["Preco_Venda"] = pd.to_numeric(vendas["Preco_Venda"], errors="coerce")
        vendas["Quantidade_Vendida"] = pd.to_numeric(vendas["Quantidade_Vendida"], errors="coerce").fillna(0).astype(int)

        # Fill Fornecedor
        produtos["Fornecedor"] = produtos["Fornecedor"].fillna("Não Informado")

        # Fill Preco_Custo by category mean
        produtos["Categoria"] = produtos["Categoria"].fillna("Não Informado")
        cat_mean = produtos.groupby("Categoria")["Preco_Custo"].transform("mean")
        # If a category mean is NaN (all missing), fill with 0 and log
        produtos["Preco_Custo"] = produtos["Preco_Custo"].fillna(cat_mean)
        if produtos["Preco_Custo"].isna().any():
            logging.warning("Some Preco_Custo remain NaN after filling with category mean; filling with 0")
            produtos["Preco_Custo"] = produtos["Preco_Custo"].fillna(0)

        # Merge vendas with produtos to get Preco_Custo when needed
        vendas_merged = vendas.merge(produtos[["ID_Produto", "Preco_Custo", "Nome_Produto", "Categoria"]], on="ID_Produto", how="left")

        # Fill missing Preco_Venda with Preco_Custo * 1.3
        vendas_merged["Preco_Venda"] = vendas_merged.apply(
            lambda row: row["Preco_Venda"] if pd.notna(row["Preco_Venda"]) else (row["Preco_Custo"] * 1.3 if pd.notna(row["Preco_Custo"]) else 0),
            axis=1,
        )

        # Calculate Receita_Total and Margem_Lucro
        vendas_merged["Receita_Total"] = vendas_merged["Quantidade_Vendida"] * vendas_merged["Preco_Venda"]
        vendas_merged["Margem_Lucro"] = vendas_merged["Preco_Venda"] - vendas_merged["Preco_Custo"]

        # Mes_Venda
        vendas_merged["Data_Venda"] = pd.to_datetime(vendas_merged["Data_Venda"], errors="coerce")
        vendas_merged["Mes_Venda"] = vendas_merged["Data_Venda"].dt.strftime("%Y-%m")

        # Build processed dataframes
        produtos_proc = produtos.copy()
        vendas_proc = vendas_merged[["ID_Venda", "ID_Produto", "Quantidade_Vendida", "Preco_Venda", "Data_Venda", "Canal_Venda", "Receita_Total", "Mes_Venda"]].copy()

        relatorio = vendas_merged[["ID_Venda", "Nome_Produto", "Categoria", "Quantidade_Vendida", "Receita_Total", "Margem_Lucro", "Canal_Venda", "Mes_Venda"]].copy()

        # Bonus: produtos baixa performance (less than 2 vendas)
        vendas_count = vendas_proc.groupby("ID_Produto").agg(Quantidade_Vendas_Total=("Quantidade_Vendida", "sum"), Vendas_Count=("ID_Venda", "count")).reset_index()
        baixa_perf = vendas_count[vendas_count["Vendas_Count"] < 2].merge(produtos_proc, on="ID_Produto", how="left")

        # Convert to JSON for XCom transport
        return {
            "produtos_proc": produtos_proc.to_json(orient="records", date_format="iso"),
            "vendas_proc": vendas_proc.to_json(orient="records", date_format="iso", date_unit="s"),
            "relatorio": relatorio.to_json(orient="records", date_format="iso"),
            "baixa_perf": baixa_perf.to_json(orient="records", date_format="iso"),
        }

    @task()
    def create_tables() -> None:
        hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
        sqls = [
            """
            CREATE TABLE IF NOT EXISTS produtos_processados (
                ID_Produto VARCHAR(10),
                Nome_Produto VARCHAR(100),
                Categoria VARCHAR(50),
                Preco_Custo DECIMAL(10,2),
                Fornecedor VARCHAR(100),
                Status VARCHAR(20),
                Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS vendas_processadas (
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
            """,
            """
            CREATE TABLE IF NOT EXISTS relatorio_vendas (
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
            """,
            """
            CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
                ID_Produto VARCHAR(10),
                Vendas_Count INTEGER,
                Quantidade_Vendas_Total INTEGER,
                Nome_Produto VARCHAR(100),
                Categoria VARCHAR(50),
                Preco_Custo DECIMAL(10,2),
                Fornecedor VARCHAR(100),
                Status VARCHAR(20),
                Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
        ]
        for s in sqls:
            hook.run(s)
        logging.info("Tables created or verified")

    @task()
    def load_data(processed: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)

        produtos_df = pd.read_json(processed["produtos_proc"], convert_dates=False)
        vendas_df = pd.read_json(processed["vendas_proc"], convert_dates=True)
        relatorio_df = pd.read_json(processed["relatorio"], convert_dates=False)
        baixa_df = pd.read_json(processed["baixa_perf"], convert_dates=False)

        # Ensure column names/order match table definitions
        # produtos_processados
        if not produtos_df.empty:
            cols_prod = ["ID_Produto", "Nome_Produto", "Categoria", "Preco_Custo", "Fornecedor", "Status"]
            _df = produtos_df.reindex(columns=cols_prod)
            _df.columns = cols_prod
            _df = _df.fillna("")
            _df.to_csv("/tmp/produtos_processados.csv", index=False)
            _df_sql = _df.copy()
            _df_sql["Preco_Custo"] = pd.to_numeric(_df_sql["Preco_Custo"], errors="coerce").fillna(0)
            _df_sql["Preco_Custo"] = _df_sql["Preco_Custo"].round(2)
            _df_sql.columns = cols_prod
            _df_sql = _df_sql.replace({pd.NA: ""})
            _df_sql = _df_sql.where(pd.notnull(_df_sql), None)
            _df_sql = _df_sql.astype(object)
            # Write using COPY
            _df_sql_for_copy = _df_sql.copy()
            _df_sql_for_copy.to_csv(index=False)
            try:
                _df_to_postgres_copy(_df_sql, "produtos_processados", hook)
            except Exception as e:
                logging.warning(f"COPY into produtos_processados failed: {e}; falling back to insert_rows")
                hook.insert_rows(table="produtos_processados", rows=_df_sql.values.tolist(), target_fields=cols_prod)

        # vendas_processadas (ensure date is in YYYY-MM-DD)
        if not vendas_df.empty:
            cols_vendas = ["ID_Venda", "ID_Produto", "Quantidade_Vendida", "Preco_Venda", "Data_Venda", "Canal_Venda", "Receita_Total", "Mes_Venda"]
            _v = vendas_df.reindex(columns=cols_vendas).copy()
            _v["Data_Venda"] = pd.to_datetime(_v["Data_Venda"]).dt.date
            _v["Preco_Venda"] = pd.to_numeric(_v["Preco_Venda"], errors="coerce").fillna(0).round(2)
            _v["Receita_Total"] = pd.to_numeric(_v["Receita_Total"], errors="coerce").fillna(0).round(2)
            try:
                _df_to_postgres_copy(_v, "vendas_processadas", hook)
            except Exception as e:
                logging.warning(f"COPY into vendas_processadas failed: {e}; falling back to insert_rows")
                hook.insert_rows(table="vendas_processadas", rows=_v.values.tolist(), target_fields=cols_vendas)

        # relatorio_vendas
        if not relatorio_df.empty:
            cols_rel = ["ID_Venda", "Nome_Produto", "Categoria", "Quantidade_Vendida", "Receita_Total", "Margem_Lucro", "Canal_Venda", "Mes_Venda"]
            _r = relatorio_df.reindex(columns=cols_rel).copy()
            _r["Receita_Total"] = pd.to_numeric(_r["Receita_Total"], errors="coerce").fillna(0).round(2)
            _r["Margem_Lucro"] = pd.to_numeric(_r["Margem_Lucro"], errors="coerce").fillna(0).round(2)
            try:
                _df_to_postgres_copy(_r, "relatorio_vendas", hook)
            except Exception as e:
                logging.warning(f"COPY into relatorio_vendas failed: {e}; falling back to insert_rows")
                hook.insert_rows(table="relatorio_vendas", rows=_r.values.tolist(), target_fields=cols_rel)

        # produtos_baixa_performance
        if not baixa_df.empty:
            cols_baixa = ["ID_Produto", "Vendas_Count", "Quantidade_Vendas_Total", "Nome_Produto", "Categoria", "Preco_Custo", "Fornecedor", "Status"]
            _b = baixa_df.copy()
            # Ensure columns exist
            for c in cols_baixa:
                if c not in _b.columns:
                    _b[c] = None
            _b = _b[cols_baixa]
            try:
                _df_to_postgres_copy(_b, "produtos_baixa_performance", hook)
            except Exception as e:
                logging.warning(f"COPY into produtos_baixa_performance failed: {e}; falling back to insert_rows")
                hook.insert_rows(table="produtos_baixa_performance", rows=_b.values.tolist(), target_fields=cols_baixa)

        # Validate counts
        results = {}
        for t, df in [("produtos_processados", produtos_df), ("vendas_processadas", vendas_df), ("relatorio_vendas", relatorio_df), ("produtos_baixa_performance", baixa_df)]:
            try:
                cnt = hook.get_first(f"SELECT COUNT(*) FROM {t}")[0]
            except Exception:
                cnt = None
            results[t] = {"expected": len(df), "in_db": cnt}

        logging.info(f"Load results: {results}")
        return results

    @task()
    def generate_report(processed: dict) -> dict:
        # Generate local aggregated report and log the outcomes
        rel = pd.read_json(processed["relatorio"], convert_dates=False)

        report = {}
        if rel.empty:
            logging.info("No relatorio rows to generate report")
            return report

        # Total de vendas por categoria
        total_por_categoria = rel.groupby("Categoria")["Receita_Total"].sum().sort_values(ascending=False).to_dict()

        # Produto mais vendido (por quantidade)
        mais_vendido = rel.groupby("Nome_Produto")["Quantidade_Vendida"].sum().sort_values(ascending=False)
        produto_mais_vendido = mais_vendido.index[0] if not mais_vendido.empty else None

        # Canal de venda com maior receita
        canal_mais_receita = rel.groupby("Canal_Venda")["Receita_Total"].sum().sort_values(ascending=False)
        canal_top = canal_mais_receita.index[0] if not canal_mais_receita.empty else None

        # Margem média por categoria
        margem_media = rel.groupby("Categoria")["Margem_Lucro"].mean().round(2).to_dict()

        report["total_por_categoria"] = total_por_categoria
        report["produto_mais_vendido"] = produto_mais_vendido
        report["canal_mais_receita"] = canal_top
        report["margem_media_por_categoria"] = margem_media

        logging.info("Relatório gerado:")
        logging.info(report)

        # Optionally save report to data/reports
        try:
            out_dir = os.path.join(os.getcwd(), "data", "reports")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            pd.Series(report).to_json(out_path)
            logging.info(f"Report saved to {out_path}")
        except Exception as e:
            logging.warning(f"Could not save report to disk: {e}")

        return report

    # DAG wiring
    produtos_raw = extract_produtos()
    vendas_raw = extract_vendas()
    processed = transform_data(produtos_raw, vendas_raw)
    create_tables()
    load_results = load_data(processed)
    report = generate_report(processed)


dag = pipeline_produtos_vendas()
