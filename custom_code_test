import json
import logging
import sys

import requests
from pyspark.sql import SparkSession

from git import Repo

spark = SparkSession.builder \
    .appName("PipelineExecutionReport") \
    .getOrCreate()

git_credential_id = int(spark.conf.get("spark.nabu.git_credential_id"))
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



git_username, git_token = fetch_credentials(credential_id= git_credential_id, credential_type_id= 19, key_map=('username', 'pat'))

repo_name = "publicRepo"
branch = "main"  # or 'master'


repo_url = f"https://{git_token}@github.com/{git_username}/{repo_name}.git"


destination_path = f"./{repo_name}"


print("Cloning...")
Repo.clone_from(repo_url, destination_path, branch=branch)
print(f"✅ Cloned into: {destination_path}")
