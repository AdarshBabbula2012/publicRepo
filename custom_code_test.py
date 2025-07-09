import logging
import sys
import traceback
from urllib.parse import urlparse

import psycopg2
import urllib3
from pyspark.sql import SparkSession
import re
import json
import requests
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
        raise ValueError(f"Invalid Kosh JDBC URL: {jdbc_url}")
    host, port, dbname = match.groups()
    return host, port, dbname

def get_pipeline_id(conn, pipeline_name):
    query = f"""
            SELECT data_movement_id from nabu.data_movement_physical where data_movement_name='{pipeline_name}'  and valid_to_ts = '9999-12-31 00:00:00.000'
        """

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        return result[0]
    else:
        raise Exception(f"No pipeline found for name like '{pipeline_name}'.")

def get_credential_name(conn, credential_id):
    query = f"""
            select credential_name from nabu.credential_info c where c.credential_id={credential_id}  and valid_to_ts = '9999-12-31 00:00:00.000'
        """

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        return result[0]
    else:
        raise Exception(f"No credential found for id like '{credential_id}'.")

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


config["fire_shots_url"] = spark.conf.get("spark.nabu.fireshosts_end_point")


config["kosh"] ={}

config["kosh"]["host"], config["kosh"]["port"], config["kosh"]["database"] = parse_postgres_jdbc_url(kosh_url)

kosh_credential_id = int(spark.conf.get("spark.nabu.kosh_credential_id"))
kosh_credential_type_id = 1

config["kosh"]["user_name"], config["kosh"]["password"] = fetch_credentials(kosh_credential_id, kosh_credential_type_id, key_map=('username', 'password'))


config["pdf_generation_pipeline"] ={}

config["pdf_generation_pipeline"]["pipeline_name"] = spark.conf.get("spark.nabu.report_generation_pipeline")

kosh_conn = get_kosh_connection(host=config["kosh"]["host"], port=config["kosh"]["port"],
                                    database=config["kosh"]["database"], user_name=config["kosh"]["user_name"],
                                    password=config["kosh"]["password"])

kosh_credential_name = get_credential_name(conn=kosh_conn, credential_id=kosh_credential_id)

config["kosh"]["credential_name"] = kosh_credential_name

config["kosh"]["credential_id"] = kosh_credential_id

config["pdf_generation_pipeline"]["pipeline_id"] = get_pipeline_id(conn=kosh_conn, pipeline_name=config["pdf_generation_pipeline"]["pipeline_name"])

print("config: " , config)


