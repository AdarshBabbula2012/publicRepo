import json
from pyspark.sql import SparkSession
import requests
import logging
import sys

spark = SparkSession.builder \
    .appName("Adarsh Integration") \
    .getOrCreate()


sf_credential_id = int(spark.conf.get("spark.nabu.credential_id"))
sf_credential_type_id = int(spark.conf.get("spark.nabu.credential_type_id"))
token = spark.conf.get("spark.nabu.token")
credential_endpoint_url = spark.conf.get("spark.nabu.fireshots_url")

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

sfUser, sfPassword = fetch_credentials(sf_credential_id, sf_credential_type_id, key_map=('username', 'password'))
