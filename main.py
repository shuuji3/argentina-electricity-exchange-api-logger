# Save Argenrina's four electricity exchange information to Google Datastore
import datetime

import requests
from google.cloud import datastore
from google.oauth2 import service_account

API_ENDPOINT = "https://api.cammesa.com/demanda-svc/demanda/IntercambioCorredoresGeo"


def main():
    credentials = service_account.Credentials.from_service_account_file(
        "service-account.json"
    )
    datastore_client = datastore.Client(credentials=credentials)

    data = fetch_data()
    put_data(datastore_client, data)


def fetch_data():
    r = requests.get(API_ENDPOINT)
    data = []
    for feature in r.json()["features"]:
        if not feature["properties"]["internacional"]:
            continue
        data.append(feature["properties"])
    return data


def put_data(datastore_client, data):
    kind = "argentina-electricity-exchange-api-logger"
    timestamp = datetime.datetime.now().replace(microsecond=0)
    timestamp = timestamp.strftime("%Y-%m-%d_%H:%M")

    key = datastore_client.key(kind, timestamp)

    entity = datastore.Entity(key=key)
    entity["data"] = data
    datastore_client.put(entity)

    print(f"wrote {entity}")


if __name__ == "__main__":
    main()
