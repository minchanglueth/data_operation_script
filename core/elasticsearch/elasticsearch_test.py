from elasticsearch import Elasticsearch
import json
es = Elasticsearch(HOST="http://localhost", PORT=9200)
es = Elasticsearch()

# es.indices.get_alias("*")
# for index in es.indices.get('*'):
#   print(index)

indices_names = ["conversation_message"]
dict_index_fields = {}


for index in indices_names:
    mapping = es.indices.get_mapping(index)
    dict_index_fields[index] = []
    for field in mapping[index]['mappings']['properties']:
        dict_index_fields[index].append(field)
    print(dict_index_fields)


query = {
  "query": {
    "bool": {
      "must": [
          {"match": {"doc.created_at": "2020-11-26T09"}}
      ]
    }
  }
}


res = es.search(index="conversation_message", body= query, size= 10000, from_=0)
count = 0
for doc in res['hits']['hits']:
    joy = json.dumps(doc)
    count = count + 1
    # print(resp)
    print(joy)

print(count)


# joy = json.dumps(res)
# print(joy)









