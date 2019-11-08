#!/bin/python3

# Importing data from a csv into Elasticsearch

from elasticsearch import Elasticsearch
import pandas as pd

es_url = "http://localhost:9200/" # Data is loaded into Elasticsearch running in your localhost

def elasticstack_py():

    es = Elasticsearch(es_url)
    df = pd.read_csv("C:/Users/josedahlson/Desktop/Project1/data.csv")
    #print(df)

# Create mappings
    mapping = {
        "mappings": {
            "properties": {
                "device": {"type": "keyword"},
                "interface": {"type": "keyword"},
                "pop": {"type": "keyword"},
                "clli": {"type": "keyword"},
                "city": {"type": "keyword"},
                "state": {"type": "keyword"},
                "country": {"type": "keyword"},
                "region": {"type": "keyword"},
                "zone": {"type": "keyword"},
                "latitude": {"type": "long"},
                "longitude": {"type": "long"},
                "src_location": {"type": "geo_point"},
                "dst_location": {"type": "geo_point"},
                "tx_pct_max": {"type": "float"},
                "rx_pct_max": {"type": "float"},
                "tx_rx_pct_max": {"type": "float"},
                "errors": {"type": "float"},
                "discards": {"type": "float"},
                "composite": {"type": "float"},
                "epoch_time": {"type": "date",
                               "format": "epoch_millis"}
                }
            }
        }

    index = "jose_network" # Create any index name of your choice
 
    if es.indices.exists(index): # Choose to use or delete an existing index
        print("\n" + str(index) + " index found")
        #print("deleting index")
        #es.indices.delete(index, ignore=[400, 404])

    else:
        print("\nCreating index " + str(index))
        res = es.indices.create(index, ignore=400, body=mapping) # Create a new index
        #print(res)

    print("Inserting data into elasticsearch...")
    
    for each_row in (df.to_dict('records')):
        #print(each_row)
        es.index(index, body=each_row)
   
    print("\nScript executed successfully!!")

elasticstack_py()
