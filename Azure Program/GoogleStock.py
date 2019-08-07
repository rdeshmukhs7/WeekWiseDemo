# Databricks notebook source
import numpy as np
import pandas as pd

dataset_train = pd.read_csv('https://storagecgtraining.blob.core.windows.net/container-training/Google_Stock_Price_Train.csv')
X = dataset_train.iloc[:,2:3].values
y = dataset_train.iloc[:,1].values
print(dataset_train.head())

# COMMAND ----------

from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(X,y)

# COMMAND ----------

import pickle
filename = 'Google_Stock_finalized_model1.sav'
pickle.dump(regressor,open(filename,'wb'))

# COMMAND ----------

dataset_test = pd.read_csv('https://storagecgtraining.blob.core.windows.net/container-training/Google_Stock_Price_Test.csv')
X_test = dataset_test.iloc[:,2:3].values
y_pred = regressor.predict(X_test)
print(y_pred)

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip install --upgrade pip

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip install azure

# COMMAND ----------

from azure.storage.blob import BlockBlobService, PublicAccess

block_blob_service = BlockBlobService(account_name='storagecgtraining', account_key='storageaccountkey')

container_name ='container-training'
block_blob_service.create_container(container_name)

# Set the permission so the blobs are public.
block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)

# Upload the created file, use local_file_name for the blob name
block_blob_service.create_blob_from_path(container_name, filename, filename)

# COMMAND ----------

filename1='test.sav'
block_blob_service.get_blob_to_path(container_name,filename,filename1)
loaded_model = pickle.load(open(filename1, 'rb'))
result = loaded_model.predict(X_test)
print(result)

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip install azure-mgmt-cosmosdb

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip install azure.cosmos

# COMMAND ----------

import azure.cosmos.cosmos_client as cosmos_client

config = {
    'ENDPOINT': 'cosmosdb-url',
    'PRIMARYKEY': 'cosmosdbkey',
    'DATABASE': 'CosmosDatabase',
    'CONTAINER': 'CosmosContainer'
}

# Initialize the Cosmos client
client = cosmos_client.CosmosClient(url_connection=config['ENDPOINT'], auth={'masterKey': config['PRIMARYKEY']})


# COMMAND ----------

# Create a database
db = client.CreateDatabase({ 'id': config['DATABASE'] })

# COMMAND ----------

# Create container options
options = {
    'offerThroughput': 400
}

container_definition = {
    'id': config['CONTAINER']
}

# COMMAND ----------

# Create a container
container = client.CreateContainer(db['_self'], container_definition, options)

# COMMAND ----------

# Create and add some items to the container
item1 = client.CreateItem(container['_self'], {
    'id': 'server1',
    'Web Site': 0,
    'Cloud Service': 0,
    'Virtual Machine': 0,
    'message': 'Hello World from Server 1!'
    }
)

item2 = client.CreateItem(container['_self'], {
    'id': 'server2',
    'Web Site': 1,
    'Cloud Service': 0,
    'Virtual Machine': 0,
    'message': 'Hello World from Server 2!'
    }
)

# COMMAND ----------

# Query these items in SQL
query = {'query': 'SELECT * FROM server s'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 2

result_iterable = client.QueryItems(container['_self'], query, options)
for item in iter(result_iterable):    
    print(item['message'])

# COMMAND ----------

# Store predicted output in cosmos db using python code

options = {
    'offerThroughput': 400
}

container_definitionGS = {
    'id': 'ContainerGoogleStock'
}

containerGS = client.CreateContainer(db['_self'],container_definitionGS,options)


# COMMAND ----------

for i in range(0,len(result)):   
  item1 = client.CreateItem(containerGS['_self'], {
      'id': str(i),
      'x_test' : str(X_test[i][0]),
      'y_pred' : str(result[i])
      }             
  )  

# COMMAND ----------

# Query these items in SQL
query = {'query': 'SELECT * FROM server s'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 2

result_iterable = client.QueryItems(containerGS['_self'], query, options)
print("ID","\t","X_test","\t","y_pred")
for item in iter(result_iterable):  
  print(item['id'],"\t",item['x_test'],"\t",item['y_pred'])

# COMMAND ----------


