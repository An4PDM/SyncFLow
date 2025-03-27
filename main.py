from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from varejo.config import HOST,USER,PASSWORD,DATABASE,API_URL
import mysql.connector
import pandas as pd
import requests

# Extração do banco de dados
def extract_bd (**kwargs):
    conn = mysql.connector.connect(
        host = HOST,
        user = USER,
        password = PASSWORD,
        database = DATABASE
    )

    cursor = conn.cursor()

    queries = {
        'soma_estoque': 'SELECT * FROM soma_estoque;',
        'infos_de_precos': 'SELECT * FROM infos_de_precos;',
        'fornecedores':'SELECT * FROM fornecedor;',
        'produtos': 'SELECT * FROM produto;'
    }

    dataframes = {} # Para armazenar todos os DFs

    for file_name, query in queries.items():
        cursor.execute(query)
        columns=[desc[0] for desc in cursor.description] # Retorna a primeira linha (nome das colunas)
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(f'spreadsheets/{file_name}.csv', index=False)

        # Salvando no dicionario geral como serializável
        dataframes[file_name] = df.to_json()

    cursor.close()
    conn.close()

    kwargs['ti'].xcom_push(key='df', values=dataframes)

def extract_api (**kwargs):
    url = API_URL
    response = requests.get(url)

    if response.status_code == 200:
        pedidos = response.json()

        # Transformando em dataframe
        df_api = pd.DataFrame(pedidos)

        # Salvando em csv temporário
        df_api.to_csv('pedidos.csv', index=False)

    else:
        raise Exception (f'Erro ao acessar API: {response.status_code}')
    

# Transformação
def transform_data_from_db (**kwargs):
    ti = kwargs['ti']
    df_serial = ti.xcom_pull(key='df',task_ids='Extract_bd')

    # Alterando arquivo serializável para dataframe
    df_database = pd.DataFrame(df_serial)

    





# Carregamento


with DAG (
    dag_id='Varejo',
    schedule='@daily',
    start_date=datetime(2025,3,12),
    catchup=True
) as dag:
    
    start = EmptyOperator(task_id='Start')
    extracting_bd = PythonOperator(task_id='Extract_bd', python_callable=extract_bd)
    extracting_api = PythonOperator(task_id='Extract_api', python_callable=extract_api)
    end = EmptyOperator(task_id='End')

start >> extracting_bd >> extracting_api >> end
