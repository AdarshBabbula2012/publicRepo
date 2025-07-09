import logging
import sys
import traceback
from urllib.parse import urlparse

import urllib3
from pyspark.sql import SparkSession
import re
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_kosh_connection(host, port, database, user_name, password):
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user_name,
        password=password,
    )
    return conn

def parse_postgres_jdbc_url(jdbc_url):
    pattern = r"jdbc:postgresql://([^:/]+):(\d+)/([^?]+)"
    match = re.match(pattern, jdbc_url)
    if not match:
        raise ValueError(f"Invalid PostgreSQL JDBC URL: {jdbc_url}")
    host, port, dbname = match.groups()
    return host, port, dbname

def fetch_credentials(credential_id, credential_type_id, key_map=('username', 'password')):
    try:
        headers = {
            'Authorization': token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        payload = json.dumps({
            "credential_id": credential_id,
            "credential_type_id": credential_type_id
        })

        response = requests.post(credential_endpoint_url, headers=headers, data=payload)
        response_json = response.json()

        if response.status_code != 200 or 'expired' in str(response_json.get("data", "")):
            raise Exception(response_json.get("error_msg") or response_json.get("data"))

        data = response_json.get('data', {})
        return data[key_map[0]], data[key_map[1]]

    except Exception as e:
        logging.error(f"Failed to fetch credentials: {e}")
        sys.exit(1)


spark = SparkSession.builder \
    .appName("PipelineExecutionReport") \
    .getOrCreate()

config = {}

git_credential_id = int(spark.conf.get("spark.nabu.git_credential_id"))
token = spark.conf.get("spark.nabu.token")
credential_endpoint_url = spark.conf.get("spark.nabu.fireshots_url")
kosh_url = spark.conf.get("spark.nabu.kosh_url")
parsed = urlparse(credential_endpoint_url)

config["fire_shots_url"] = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"

print("fireshots_url: " , config["fire_shots_url"])

config["kosh"] ={}

config["kosh"]["host"], config["kosh"]["port"], config["kosh"]["database"] = parse_postgres_jdbc_url(kosh_url)

kosh_credential_id = int(spark.conf.get("spark.nabu.kosh_credential_id"))
kosh_credential_type_id = 1

config["kosh"]["user_name"], config["kosh"]["password"] = fetch_credentials(kosh_credential_id, kosh_credential_type_id, key_map=('username', 'password'))

print(config)
