from connect_api_spotify import get_auth_header, get_token
from google.cloud import bigquery
from requests import get
import pandas as pd 
import os
import pyarrow #para o job do bigquery funcionar

credential_path = os.getenv("PATH")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path


client_BigQuery = bigquery.Client()
token = get_token()
headers = get_auth_header(token)
#result = search_for_podcast(token, "data hackers")



id_list = []
name_list = []
description_list = []
release_date_list = []
duration_ms_list = []
language_list = []
explicit_list = []
type_list = []



#def search_for_episode(token, podcast_name):

def search_for_episodes(token, id_podcast):

    market = "BR"
    limit = 50
    offset = 0

    runs = 1
    count = 0

    while ((offset <= 500)&(count<=runs)):
    
        query = f'https://api.spotify.com/v1/shows/{id_podcast}/episodes?'
        #query += f'&q={q}'
        #query += f'&type={type}'
        query += f'&offset={offset}'
        query += f'&market={market}'
        query += f'&limit={limit}'

        result = get(query, headers=headers)

        json_result = result.json()

        offset = offset + 50
        count += 1

        for i in range(len(json_result['items'])):

            id_list.append(json_result['items'][i]['id'])
            name_list.append(json_result['items'][i]['name'])
            description_list.append(json_result['items'][i]['description'])
            release_date_list.append(json_result['items'][i]['release_date'])
            duration_ms_list.append(json_result['items'][i]['duration_ms'])
            language_list.append(json_result['items'][i]['language'])
            explicit_list.append(json_result['items'][i]['explicit'])
            type_list.append(json_result['items'][i]['type'])
        
        runs = (json_result['total']//50)

    return json_result

search_for_episodes(token, "1oMIHOXsrLFENAeM743g93")
#    return json_result

#search_for_episode(token, "data hackers")

podcast = pd.DataFrame()
podcast['id'] = id_list
podcast['name'] = name_list
podcast['description'] = description_list
podcast['release_date'] = release_date_list
podcast['duration_ms'] = duration_ms_list #/ 60000
podcast['language'] = language_list
podcast['explicit'] = explicit_list
podcast['type'] = type_list

#print(total_episodios_list)
#podcast.to_csv('test.csv', index=False, sep=';', encoding='utf-8')

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