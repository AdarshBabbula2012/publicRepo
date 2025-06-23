import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

credential_id = int(spark.conf.get("spark.nabu.credential_id"))
credential_type_id = int(spark.conf.get("spark.nabu.credential_type_id"))

def getCredentials(credential_id,credential_type_id):
    try:
        token = spark.conf.get("spark.nabu.token")
        credential_endpoint_url = spark.conf.get("spark.nabu.fireshots_url")

        payload = json.dumps({
            "credential_id": credential_id,
            "credential_type_id": credential_type_id
        })
        headers = {
            'Authorization': token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", credential_endpoint_url, headers=headers, data=payload)
        response_json = response.json()

        if response.status_code != 200 or 'expired' in str(response_json.get("data")):
            if response.status_code == 401:
                error_msg = response_json.get("error_msg")
                raise Exception(
                    f"Failed to fetch credentials, status code: {response.status_code}, response: {error_msg}")
            else:
                data = response_json.get("data")
                raise Exception(f"Failed to fetch credentials, status code: {response.status_code}, response: {data}")

        elif response.status_code == 200 and 'User does not have ' in str(response_json.get("data")):
            data = response_json.get("data")
            raise Exception(f"Failed to fetch credentials, status code: {response.status_code}, response: {data}")

        credential_details = response_json
        username = credential_details['data']['username']
        password = credential_details['data']['password']

        return username,password
    except (ValueError, Exception) as e:
        logging.error(f"[+] Exception:  Error has occurred: {e}")
        sys.exit(1)

username,password = getCredentials(credential_id,credential_type_id)
