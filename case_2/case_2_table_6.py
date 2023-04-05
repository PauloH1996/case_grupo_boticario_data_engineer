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


#criando listas vazia pra fazer o imput dos dados
id_list = []
name_list = []
description_list = []
release_date_list = []
duration_ms_list = []
language_list = []
explicit_list = []
type_list = []


def search_for_episodes(token, id_podcast):
    headers = get_auth_header(token)    #parametro de autenticação de acesso a API 

    market = "BR"
    limit = 50 #range 0 - 50
    offset = 0 #range 0 - 2000
                                        
    runs = 1
    count = 0

    while ((offset <= 500)&(count<=runs)):
    
        query = f'https://api.spotify.com/v1/shows/{id_podcast}/episodes?'
        #query += f'&q={q}'
        #query += f'&type={type}'
        query += f'&offset={offset}'                       #paramentros da url q irá consultar(query) na API      
        query += f'&market={market}'
        query += f'&limit={limit}'

        result = get(query, headers=headers)                # envia(pedi) a requisição na API 

        json_result = result.json()                         # resposta em um arquivo .json

        offset = offset + 50                                #incrementa em 50 no offset
        count += 1                                          #incrementa em 1 no count

        for i in range(len(json_result['items'])):

            id_list.append(json_result['items'][i]['id'])
            name_list.append(json_result['items'][i]['name'])
            description_list.append(json_result['items'][i]['description'])
            release_date_list.append(json_result['items'][i]['release_date'])
            duration_ms_list.append(json_result['items'][i]['duration_ms'])     #tira do json e coloca nas listas 
            language_list.append(json_result['items'][i]['language'])
            explicit_list.append(json_result['items'][i]['explicit'])
            type_list.append(json_result['items'][i]['type'])
        
        runs = (json_result['total']//50)               #quantas rodadas de 50 ainda é necessária 

    return json_result

search_for_episodes(token, "1oMIHOXsrLFENAeM743g93")
### ID Data Hackers discovery 
#SELECT  id
# FROM `b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_5` 
# WHERE name = "Data Hackers"
# Resultado ID = 1oMIHOXsrLFENAeM743g93


episodios = pd.DataFrame()
episodios['id'] = id_list
episodios['name'] = name_list
episodios['description'] = description_list
episodios['release_date'] = release_date_list                   #lista transformada em dataframe
episodios['duration_ms'] = duration_ms_list #/ 60000
episodios['language'] = language_list
episodios['explicit'] = explicit_list
episodios['type'] = type_list

#print(total_episodios_list)
#podcast.to_csv('test.csv', index=False, sep=';', encoding='utf-8')


#### Upload dataframe como tabela no BigQuery 
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("description", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("release_date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("duration_ms", bigquery.enums.SqlTypeNames.INT64), #VEM COMO MILISECUNDOS
        bigquery.SchemaField("language", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("explicit", bigquery.enums.SqlTypeNames.BOOL),
        bigquery.SchemaField("type", bigquery.enums.SqlTypeNames.STRING),
    ],
    write_disposition="WRITE_TRUNCATE",
)
table_id = "b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_6"

job = client_BigQuery.load_table_from_dataframe(
    episodios, 
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