from connect_api_spotify import get_auth_header, get_token
from google.cloud import bigquery
from requests import get
import pandas as pd 
import os
import pyarrow #para o job do bigquery funcionar

credential_path = os.getenv("PATH")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

#autenticações
client_BigQuery = bigquery.Client()
token = get_token()
#result = search_for_podcast(token, "data hackers")

#paramentros da url q irá consultar(query) na API 
endpoint_url = "https://api.spotify.com/v1/search?"
type = "show"
market = "BR"
limit = 50
offset = 0

#criando listas vazia pra fazer o imput dos dados
name_list = []
description_list = []
id_list = []
total_episodios_list = []


def search_for_podcast(token, podcast_name):

    headers = get_auth_header(token)

    query = f'{endpoint_url}'
    query += f'&q={podcast_name}'
    query += f'&type={type}'
    query += f'&offset={offset}'
    query += f'&market={market}'
    query += f'&limit={limit}'
    
     # envia(pedi) a requisição na API
    result = get(query, headers=headers)                                      
    
    # resposta em um arquivo .json
    json_result = result.json()

    #repete até a contagem de linhas(len) do json acabar 
    for i in range(len(json_result['shows']['items'])):

        #tira do json e coloca nas listas
        name_list.append(json_result['shows']['items'][i]['name'])
        description_list.append(json_result['shows']['items'][i]['description'])
        id_list.append(json_result['shows']['items'][i]['id'])
        total_episodios_list.append(json_result['shows']['items'][i]['total_episodes'])

    #retonar o json           
    return json_result

search_for_podcast(token, "data hackers")

podcast = pd.DataFrame()
podcast['name'] = name_list
podcast['description'] = description_list
podcast['id'] = id_list
podcast['total_episodios'] = total_episodios_list

#print(total_episodios_list)
#podcast.to_csv('test.csv', index=False, sep=';', encoding='utf-8')

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("description", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("total_episodios", bigquery.enums.SqlTypeNames.INT64),
    ],
    write_disposition="WRITE_TRUNCATE",
)
table_id = "b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_5"

job = client_BigQuery.load_table_from_dataframe(
    podcast, 
    table_id, 
    job_config=job_config
)
job.result()

table = client_BigQuery.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)