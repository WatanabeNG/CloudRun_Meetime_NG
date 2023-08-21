from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import json
import pandas as pd
import requests

app = FastAPI()
project_id = "sys-67738525349545571962304921"
cred={'type': 'service_account', 'project_id': 'sys-67738525349545571962304921', 'private_key_id': '91927d3950bfa5e5249a0cc4f42d4625a99f4503', 'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1BaE36Via8sak\noerV17EZzGlXeBjgsscbVnLuyg6ACDlY7CrCljjdOfmn+4gKbe1491WwJtxuhr/0\ne4od5jt2HrKTfR5yiFb3EujfcZrl7kA8OPnSp7Z5StZoos4AHAMJBcgGGMfbHVA4\nQ+ZPjXXSIGWJdh8/SV0Es51VUQi+Lms3YK0fzufME4qceS9nybYmm3ccrZSG51zn\nhK7SNmRZZCZIfOQGmXa/hqZw17B1TFKIR9PefkYtWI8yTOvsZ6crzttiouQzulOl\n5DQwmbfOeEWMKT6HoXPTa+V381S7WRAK8YF98+DVqerjBPt3DND+/YL2ZwA/zqJz\ne5iz2D7jAgMBAAECggEADbW48ZabLtUPRV27/ukgkR8hpU3DuJThrojcGIi2E21M\nBpeQX39oHB0tctMCiSOtNhmpZDd1P2u2Mwp+Oeh7fWUyyifSPANmbr0AZRfiDuL9\n+3GnPhSUpdgMqA0Yg/qbIj5NWWTcEhTExBYkZccFct4gQopvMGhagqYl1tXVzy1t\nNpE0Ge+uishasM0AE0c6eGppyfm7zb/Df0Q6vDfmGHSiO4WCwLjUm9BayGS68Pt1\n7MTOqO3B5Eo4KTcahCY41+iXvu8K2EZ1DRRS9xvBs0pBqK68fSuQSMSUzF18CHzb\n5tg/OgLWQxiSs8jOLUHox6k4dkF2NejqYh3plCsnQQKBgQDYjtcAWlbCalDKWjX8\noV3YOuwY1LrhsF8IuCnynkAlKKrleZHmbFubrYN6C1GDKo4QPM5j7IuHRXEz8Pfv\nsym9VJPeOqE0NBy8HQV3NtLzwypaNUlGi250uSEpddSS7p0AL7p5mhSet9aTSdBq\n6cSVbm13GFkjoFvwir+kEFRCwQKBgQDV/ea10Npmf8IBKO23d5Is5xMvHry8MzZI\noVpFWPym5HS5XEA06APDd3ccyCndDcJQAJld6C3CLbTxZdWhg2WGSpWnBarlSVQh\nUcRawHAMvSR0VGeUjOuQvNecutpEx5u6qs2AaT7kj2fQyOnH3Ux1w+GbtytdhOfI\nyBjIgsc+owKBgGrlZ1+3OChTjnm0Of3wMYCw5SYErBMHmoGVVq96SjONdX48mjZh\nun6IEeRGff//G40MVtygQOeO8agwBFL/31Sj0THbQwOfzadVtAL6vvqwldFdiEQY\nQ3e+go4SqdG1ky4qYSPxWMhX+sVNpGGB7xXMIqCtFiMt3vRHqP11SgKBAoGBAIDk\n3Xl4YoTIwV+XepA+6oI3cVu5hO9LXZAj+E67CfuwsgoQYfA8LEApjkp82pJ2visY\nIUjqF93VUB7zOtl9XsKj3D5tcIGJSK6FJOOQ9C0IJJQZXwagVyeoR6r09ZHmNYwb\nY4rMWgCrzFl7Gy2yw2JP6W20x98dtcs/k4X7F+5HAoGAXLQcxSEn3yDqYcqqihTd\nCwwj/C8PCVtExuY2nbd5K72G6DeJOzqq5XRAYM+ONPlofSBioVXtxTMvdKDTR8xY\njnuU/XunKfMBJfboGxXkdHL7LJNuqE8DRG3El+TFGA/aCjFSLAKZyYGmD7H2FXr4\nVYxLh0cycwfaBLeKUC9Sg2A=\n-----END PRIVATE KEY-----\n', 'client_email': 'python-2@sys-67738525349545571962304921.iam.gserviceaccount.com', 'client_id': '116817448104344134353', 'auth_uri': 'https://accounts.google.com/o/oauth2/auth', 'token_uri': 'https://oauth2.googleapis.com/token', 'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs', 'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/python-2%40sys-67738525349545571962304921.iam.gserviceaccount.com', 'universe_domain': 'googleapis.com'}
credentials = service_account.Credentials.from_service_account_info(cred)

 # Auth das 2 ferramentas do GCP
def auth_bq():
    client_bq = bigquery.Client(project=project_id, credentials=credentials)

    return client_bq

def auth_gcs():
    client_gcs = storage.Client(project=project_id, credentials=credentials)

    return client_gcs

# Configurações da API do Meetime Rh Gestor
base_url = "https://api.meetime.com.br"
headers = {
    "Authorization": "AU0HCQ4jO3aKEeVqPWuJVwNdawxvIHwx"
}

def collect_meetime_data(endpoint: str):
    if endpoint == '/v2/users?':
        request_endpoint = endpoint
    else:
        request_endpoint = endpoint + "limit=100&start=0"    

    data = []
    print(f"{base_url}{request_endpoint}")
    while next_page:

        response = requests.get(f"{base_url}{request_endpoint}", headers=headers)

        if response.status_code == 200:
            json_data = response.json()
            data.extend(json_data["data"])
            next_page = json_data.get("next")
            print(next_page)
            if next_page is not None :
                request_endpoint = json_data.get("next")
        else:
            raise HTTPException(status_code=response.status_code, detail=response)
    print(len(data))
    return data


collect_meetime_data('/v2/prospections?')


@app.post("/collect-meetime-data")
def collect_and_load_data():
    client = bigquery.Client(project=project_id)
    data_to_load = collect_meetime_data()

    if data_to_load:
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        errors = client.insert_rows_json(table_ref, data_to_load)

        if not errors:
            return {"message": "Dados carregados com sucesso no BigQuery"}
        else:
            return {"message": "Erro ao carregar dados no BigQuery", "errors": errors}
    else:
        return {"message": "Nenhum dado coletado da API do Meetime"}

# Código de deploy para o Cloud Run
# Certifique-se de configurar as variáveis de ambiente corretamente

# gcloud run deploy nome-do-app --image gcr.io/nome-do-projeto/imagem:tag


@app.get("/collect-and-load-bq")
async def collect_and_load_bq_data():
    # Lista de endpoints da API do Meetime
    endpoints = ["/v2/USERS?", "/v2/COMPANY?"]

    client_bq = auth_bq()
    clint_gcs = auth_gcs()

    # Coleta de dados de forma assíncrona e carregamento em tabelas separadas
    for endpoint in endpoints:
        collected_data = await collect_meetime_data(endpoint)
        
        if collected_data:
            endpoint = endpoint.replace('/v2/', '')
            endpoint = endpoint.replace('?', '')
            table_id = f"MEETIME_{endpoint}"  # Criando um nome de tabela com base no endpoint
            table_ref = client.dataset(dataset_id).table(table_id)
            
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect = True
            
            errors = client.insert_rows_json(table_ref, collected_data, job_config=job_config)

            if not errors:
                print(f"Dados carregados com sucesso na tabela {table_id}")
            else:
                print(f"Erro ao carregar dados na tabela {table_id}: {errors}")
        else:
            print(f"Nenhum dado coletado do endpoint {endpoint}")

    return {"message": "Coleta e carregamento concluídos"}