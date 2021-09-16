#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json
import yaml
from kafka import KafkaProducer
from time import sleep

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


# Send message to Kafka producer
# =======================================================================================================
producer = KafkaProducer(bootstrap_servers=[configuration['KAFKA_BOOTSTRAP']],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))



for e in range(50):
    result = run_query(query)  # Execute the query
    print(f"Result - {result}")
    mjson = json.loads(f"{result}".replace("\'", "\""))
    data = {"blocks_count": mjson['data']['bitcoin']['blocks'][0]['count']}
    print(e)
    print(type(data))
    print(data)
    print(json.dumps(data).encode('utf-8'))
    producer.send("blocks", value=data)
    sleep(11)

# ======================================================================================================env.execute()