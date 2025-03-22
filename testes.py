
import pandas as pd
from varejo.config import HOST,USER,PASSWORD,DATABASE,API_URL
import mysql.connector
import pandas as pd


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
        'fornecedores':'SELECT * FROM fornecedor;' 
    }

    dataframes = {} # Para armazenar todos os DFs

    for file_name, query in queries.items():
        cursor.execute(query)
        columns=[desc[0] for desc in cursor.description] # Retorna a primeira linha (nome das colunas)
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(f'{file_name}.csv', index=False)

        # Salvando no dicionario geral como serializável
        dataframes[file_name] = df.to_json()

    cursor.close()
    conn.close()

    kwargs['ti'].xcom_push(key='df', values=dataframes)



