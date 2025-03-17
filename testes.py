from config import API_URL
import requests
import pandas as pd

def extract_api ():
    url = API_URL
    response = requests.get(url)

    if response.status_code == 200:
        pedidos = response.json()

        # Transformando em dataframe
        df_api = pd.DataFrame(pedidos)
        print(df_api)
        # Salvando em csv temporário
        df_api.to_csv('/tmp/pedidos.csv', index=False)
        

    else:
        raise Exception (f'Erro ao acessar API: {response.status_code}')
    
extract_api()