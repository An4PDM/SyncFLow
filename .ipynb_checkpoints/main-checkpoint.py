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

    query = 'SELECT * FROM soma_estoque;'
    cursor.execute(query)

    # Definindo colunas e linhas
    columns = [desc[0] for desc in cursor.description] #Retorna a primeira linha (nome das colunas)
    rows = cursor.fetchall()

    # Convertendo para dataframe e salvando temporariamente
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv('soma_estoque.csv', index=False)

    cursor.close()
    conn.close()

    # Alterando para arquivo serializável
    df_serial_db = df.to_json()
    kwargs['ti'].xcom_push(key='df',value=df_serial_db)

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
    schedule_interval='@daily',
    start_date=datetime(2025,3,12),
    catchup=True
) as dag:
    
    start = EmptyOperator(task_id='Start')
    extracting_bd = PythonOperator(task_id='Extract_bd', python_callable=extract_bd)
    extracting_api = PythonOperator(task_id='Extract_api', python_callable=extract_api)
    end = EmptyOperator(task_id='End')

start >> extracting_bd >> extracting_api >> end
