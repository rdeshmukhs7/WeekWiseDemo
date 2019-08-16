#pip install kafka-python

#import numpy as np
#import pandas as pd
import json
import requests
#from kafka import KafkaProducer
import time

response = requests.get("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=1min&outputsize=compact&apikey=0D8RR9NDGU7URNQT")
data = json.loads(response.text)
print(data)

producer = KafkaProducer(bootstrap_servers=['10.1.0.7','10.1.0.5','10.1.0.6'],api_version=(0,10))

for i,j in data['Time Series (1min)'].items():
     producer.send('test',value=str(j),key=str(i))   
     time.sleep(1)   
     #future = producer.send('numtest',key=i, value=j)
     #result = future.get(timeout=60) 
#producer.flush()
