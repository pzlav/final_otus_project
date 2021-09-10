#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json


def run_query(query):  # A simple function to use requests.post to make the API call.
    headers = {'X-API-KEY': 'TODO'}
    request = requests.post('https://graphql.bitquery.io/',
                            json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed and return code is {}.      {}'.format(request.status_code,
                        query))


# The GraphQL query
query = """
query{
  bitcoin{
    blocks{
      count
    }
   }
}
"""



result = run_query(query)  # Execute the query
print(f"Result - {result}")

mjson = json.loads(f"{result}".replace("\'", "\""))
print(type(mjson))
#print(mjson['data']['bitcoin']['blocks']['count'])
print(mjson['data']['bitcoin']['blocks'][0]['count'])
