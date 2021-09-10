#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json
import yaml
from kafka import KafkaProducer


# Reading configuration file
# ======================================================================================================
with open("project.yaml", "r") as stream:
    try:
        configuration = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

def run_query(query):  # A simple function to use requests.post to make the API call.
    headers = {'X-API-KEY': configuration['BITQUERY_API_KEY']}
    request = requests.post('https://graphql.bitquery.io/',
                            json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed and return code is {}.      {}'.format(request.status_code,
                        query))

# The GraphQL query
# ======================================================================================================
query = """
query{
  bitcoin{
    blocks{
      count
    }
   }
}
"""
# =======================================================================================================


result = run_query(query)  # Execute the query
print(f"Result - {result}")
mjson = json.loads(f"{result}".replace("\'", "\""))
print(mjson['data']['bitcoin']['blocks'][0]['count'])


# Send message to Kafka producer
# ======================================================================================================
producer = KafkaProducer(bootstrap_servers=[configuration['KAFKA_BOOTSTRAP']],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))