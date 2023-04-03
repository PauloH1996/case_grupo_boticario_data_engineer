from dotenv import load_dotenv
import os 
import base64
from requests import post, get 
import pandas as pd
import json
load_dotenv()

#DEFININDO CHAVE DE CLIENT
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

#print(client_id, client_secret)

#https://developer.spotify.com/documentation/web-api/tutorials/code-flow
#https://github.com/spotify/web-api-examples/blob/master/authentication/client_credentials/app.js

#GERANDO O TOKEN DE ACESSO 
def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
    #print(auth_bytes)
    #print(auth_base64)

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
    }

    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]

    return token

#AUTENTICANDO HEADER PARA ACESSAR O CONSOLE DO SPOTIFY EX. ALBUNS, ARTISTAS ...ETC 
def get_auth_header(token):
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + token
    }












